#!/usr/bin/env python3
"""
DineScores Data Pipeline
========================
Collects health inspection data from Chicago, NYC, SF, and DFW metroplex cities.
DFW cities use the MyHealthDepartment portal (inspections.myhealthdepartment.com).
Outputs:
  - data.js (--output-data-js)   embedded dataset the map loads instantly
  - D1 SQL (--output-d1-sql)     idempotent upserts for the Cloudflare D1 database

Run modes:
  --full    Pull all available data (initial load)
  --weekly  Pull only data from the last 8 days (weekly refresh)
  --test    Pull 25 records per city to verify pipeline works

DFW cities:
  Use --cities dfw for all DFW metro cities, or individual slugs like
  --cities dallas fortworth plano
"""

import os, sys, json, re, time, math, hashlib, logging, argparse, base64, zlib, gzip
import html as html_lib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from urllib.parse import urlencode, quote

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('dinescores')

# Overall fetch deadline (epoch seconds), set from --deadline-minutes. Once
# passed, remaining fetch groups are skipped and retry loops stop sleeping,
# so the run always reaches the output/merge phase. A run killed by the CI
# job timeout ships nothing at all — the merge-based outputs mean shipping
# the sources that did finish is strictly better (skipped sources keep
# their existing records).
_FETCH_DEADLINE = None


def _past_deadline():
    return _FETCH_DEADLINE is not None and time.time() > _FETCH_DEADLINE

# ─── VIOLATION CLASSIFIER ────────────────────────────────────────────────────

# Ordered dict: classify_violation() returns the FIRST category whose pattern
# matches, so the most distinctive categories must come before catch-all ones
# (e.g. pest_control before general_cleanliness, which matches r'floor').
VIOLATION_CATEGORIES = {
    'pest_control': [
        r'\bpests?\b', r'rodent', r'\brats?\b', r'\bmouse\b', r'\bmice\b',
        r'cockroach', r'roach', r'\bfly\b', r'\bflies\b', r'insect',
        r'vermin', r'\bbugs?\b', r'gnaw', r'droppings', r'bait station',
        r'\bants?\b', r'\bgnats?\b', r'maggot', r'larva', r'weevil',
    ],
    'temperature_control': [
        r'temperat', r'hot hold', r'cold hold', r'phf', r'tcs food',
        r'41\s*f', r'135\s*f', r'cooling', r'reheat', r'thaw', r'frozen',
        r'refriger', r'freezer', r'warm', r'heat', r'pasteur',
        r'time.{0,20}temperature', r'potentially hazardous',
    ],
    'cross_contamination': [
        r'cross.?contamin', r'raw.{0,20}ready.to.eat', r'separate',
        r'allergen', r'color.?cod', r'cutting board', r'raw meat',
        r'ready.to.eat', r'rte food', r'contaminat',
    ],
    'personal_hygiene': [
        r'handwash', r'hand wash', r'hand\s+sanitiz', r'glove',
        r'bare hand', r'hair restrain', r'clean outer', r'wound',
        r'sick', r'illness', r'employee health', r'sneez', r'cough',
        r'personal hygiene', r'hygiene', r'lesion', r'open sore', r'\bboils?\b',
        r'jewelry', r'\bwatch\b', r'bracelet', r'\bnails?\b',
        r'(?:open |employee )drink', r'eating.{0,25}prep', r'drinking.{0,25}prep',
    ],
    'food_source': [
        r'approved source', r'food source', r'shellfish tag', r'shell\s?stock',
        r'expir', r'mold', r'spoil', r'wholesome', r'adulterat',
        r'label', r'mushroom', r'home.?prepar', r'unsafe food',
    ],
    'water_sewage': [
        r'sewage', r'plumbing', r'backflow', r'water supply', r'hot water',
        r'cold water', r'potable', r'overhead leak', r'sewage',
        r'water pressure', r'drainage', r'wastewater',
    ],
    'food_handling': [
        r'food handling', r'food contact', r'food storage', r'date mark',
        r'discard', r'cover', r'wrap', r'utensil', r'scoop', r'protect',
        r'unwrap', r'bare.{0,15}contact',
    ],
    'equipment_utensils': [
        r'equipment', r'utensil', r'warewash', r'dishwash', r'sanitiz',
        r'clean.{0,20}surface', r'food.?contact surface', r'NSF',
        r'approved.*equipment', r'in good repair', r'smooth.*cleanable',
        r'multiuse', r'single.use',
    ],
    'employee_training': [
        r'food handler', r'food manager', r'certified', r'training',
        r'knowledge', r'food safety cert', r'servsafe', r'person in charge',
        r'person responsible',
    ],
    'facility_design': [
        r'facility', r'ventilation', r'hood', r'grease trap',
        r'toilet', r'restroom', r'hand sink', r'mop sink',
        r'three.?comp', r'three.?compartment', r'adequate light',
        r'designated area',
    ],
    'general_cleanliness': [
        r'clean', r'dirty', r'soil', r'grime', r'grease', r'encrust',
        r'floor', r'wall', r'ceiling', r'counter', r'shelf', r'rack',
        r'storage area', r'non.?food.?contact',
    ],
    'maintenance': [
        r'repair', r'broken', r'damaged', r'caulk', r'seal', r'gap',
        r'hole', r'crack', r'chip', r'rust', r'corrode', r'maintained',
        r'structural', r'paint', r'peel',
    ],
    'waste_management': [
        r'garbage', r'refuse', r'trash', r'waste', r'dumpster',
        r'litter', r'discard', r'disposal', r'recycl',
    ],
}

SEVERITY_MAP = {
    'temperature_control': 'priority',
    'cross_contamination': 'priority',
    'personal_hygiene': 'priority',
    'food_source': 'priority',
    'water_sewage': 'priority',
    'food_handling': 'priority_foundation',
    'equipment_utensils': 'priority_foundation',
    'employee_training': 'priority_foundation',
    'facility_design': 'priority_foundation',
    'general_cleanliness': 'core',
    'pest_control': 'core',
    'maintenance': 'core',
    'waste_management': 'core',
}

# ─── DFW METROPLEX JURISDICTIONS ────────────────────────────────────────────
# Each entry maps a MyHealthDepartment portal slug to its display name and defaults.
# URL pattern: https://inspections.myhealthdepartment.com/{slug}

DFW_JURISDICTIONS = {
    # Confirmed working on myhealthdepartment.com (re-verified 2026-09-15).
    # Reachable from ordinary cloud/datacenter egress, but NOT from GitHub
    # Actions: the portal 403s every GitHub-hosted runner range, so the weekly
    # CI refresh skips these unless PORTAL_PROXY_URL is set (see below).
    # score_scale: '100' = 0-100 where higher is better (Dallas, Plano);
    #              'demerit' = demerit points where LOWER is better (Frisco).
    'dallas':       {'display_name': 'Dallas',        'default_city': 'Dallas',        'state': 'TX'},
    'plano':        {'display_name': 'Plano',         'default_city': 'Plano',         'state': 'TX'},
    'frisco':       {'display_name': 'Frisco',        'default_city': 'Frisco',        'state': 'TX',
                     'score_scale': 'demerit'},
    # Fort Worth runs its own program on MHD under a hyphenated slug;
    # demerit-scored (0 = perfect, 30+ = fail).
    'fort-worth-texas': {'display_name': 'Fort Worth', 'default_city': 'Fort Worth',   'state': 'TX',
                     'score_scale': 'demerit'},
    # Tarrant County Public Health covers ~20 mid-cities between Dallas and
    # Fort Worth (Bedford, Hurst, Southlake, Grapevine, Keller, Colleyville,
    # DFW Airport, ...). Record city fields carry the actual city.
    'tarrant':      {'display_name': 'Tarrant County', 'default_city': 'Tarrant County', 'state': 'TX'},
    # Confirmed NOT on myhealthdepartment.com (checked 2026-07-05 — API returns
    # an error dict instead of records): irving, mckinney, denton, garland,
    # grandprairie, mesquite, carrollton, richardson, allen, lewisville,
    # flowermound. Arlington publishes via ArcGIS Open Data (see
    # fetch_arlington). Slugs may be non-obvious (Fort Worth is
    # 'fort-worth-texas') — recheck with hyphenated variants before ruling
    # a jurisdiction out.
}

# Non-DFW jurisdictions on the same MyHealthDepartment portal (verified live
# 2026-07-10). Same machinery as DFW_JURISDICTIONS; extra keys:
#   metro:       metro chip label ('' = none; DFW entries implicitly 'DFW')
#   score_scale: '100' (higher better) | 'demerit' (lower better) |
#                'none' (portal publishes no score — risk must come from the
#                scraped violation details; search-only records keep a
#                neutral placeholder and are refined by the detail pass)
#   path:        the real portal slug when the table key is a PSEUDO-
#                jurisdiction — one of several fetch units carved out of a
#                single portal slug (resolved by _mhd_path; the key itself
#                is only a CLI/ordering handle). Omit when key == slug.
#   programs:    programName allowlist, applied on top of _mhd_is_food_record
#                (which only drops pool/spa/septic rows — hotels, tattoo
#                studios and child care pass it).
#   exclude:     {row field: regex} — rows whose field matches are dropped
#                before parsing (_mhd_exclude_patterns; Arapahoe's mobile
#                units have no address).
#   window_days: search window length (default MHD_WINDOW_DAYS = 1; a longer
#                window loses a permit's earlier inspections, see there).
#   sort:        the search payload's sort (default MHD_DEFAULT_SORT, the
#                stable inspectionID order; None opts out). Bucket entries
#                default to the portal's own order.
#   group:       the fetch group the jurisdiction runs in (default
#                MHD_DEFAULT_GROUP, 'MHD metros'). Heavy jurisdictions run
#                under 'MHD metros (midweek)' so the workflow can give them
#                their own cron and detail-page budget; main() builds one
#                group per distinct name (_mhd_metro_groups).
#   lag_days:    how far back the weekly window starts even when current
#                (default 8; the Oregon counties publish two weeks late).
#   search_terms + purposes + city_allow: BUCKET MODE (see the Tennessee note
#                below). For every window, for every term, for every purpose,
#                the search runs with searchStr=term and filters.purpose=
#                purpose; rows are unioned by inspectionID and then kept
#                only when their `city` is in city_allow.
#
# Tennessee Department of Health runs ONE statewide portal ('tennessee') that
# publishes ~440 rows a weekday, far past the ~225-row query cap, so a plain
# date search can never be complete — and its pagination is unstable too
# (offsets 50/75 re-serve earlier pages: 175 unique rows of 225; sorted
# ASC∪DESC skips rows at establishment-name ties). Verified 2026-10-10: a
# non-empty `searchStr` (case-insensitive substring over name+address+city)
# switches the query to ONE ROW PER PERMIT — that permit's latest inspection
# matching the filters inside the window — which is what makes a complete
# fetch possible, with two consequences the bucket entries encode:
#   * window_days 1: a multi-day window collapses a permit's earlier
#     inspections away, so every window is a single day;
#   * purposes ['Routine', 'Follow-Up'] queried separately: a same-day
#     Routine+Follow-Up pair otherwise loses the Routine.
#   (Nashville 2026-10-06: Routine 48 + Follow-Up 22 = 70 = every Nashville
#   row seen in any probe; a 5-day Gwinnett window, by contrast, returned
#   exactly the sum of its single days — Gwinnett is a normal jurisdiction.)
# Each TN metro is a pseudo-jurisdiction (path 'tennessee') whose search
# terms are its city names. The substring also matches ADDRESSES ('Madison'
# hits Madisonville and '2400 Madison St' in Clarksville, 'Franklin' hits
# Johnson City addresses, 'Germantown' hits 'Germantown Pkwy' in Cordova),
# hence city_allow. Programs: keep 'Food Service Establishment' (code 605)
# only — Pools, Tattoo, Hotel, Child Care, School, Camp and Correctional
# share the feed and _mhd_is_food_record lets most of them through. Every
# food row carries TDH's official 0-100 score; detail pages carry 15-22
# observations. Do NOT "optimise" this back to one statewide paged query:
# it silently ships a ~40% sample. Never send a non-empty programName (the
# server hangs) or lat/lng (empty result); `count` is ignored (pages are 25,
# the first page can be longer) and offsets >= 225 are rejected.


def _mhd_tn_bucket(metro, cities):
    """MHD_METRO_JURISDICTIONS entry for one Tennessee metro (see the note
    above): search_terms = city_allow = the metro's city names, spelled the
    way the portal spells them ('Mt. Juliet', 'La Vergne', 'Soddy Daisy')."""
    return {'path': 'tennessee', 'display_name': f'Tennessee DOH ({metro})',
            'default_city': metro, 'state': 'TN', 'score_scale': '100',
            'metro': metro, 'programs': ['Food Service Establishment'],
            'purposes': ['Routine', 'Follow-Up'], 'window_days': 1,
            'search_terms': list(cities), 'city_allow': list(cities)}


# Virginia Department of Health publishes every district on this portal
# (verified 2026-11-02/03). The statewide 'virginia' slug is unusable — it
# is capped at 225 rows on busy days and its unsorted pages re-serve rows —
# and bucket mode is NOT viable either (searchStr collapses to one row per
# permit AND silently drops ~40% of permits). But each of the 33 health
# districts is its own sub-site ('va-richmond', 'va-norfolk', ...) and an
# ordinary, uncapped jurisdiction (at most ~20 rows a day), whose rows sum
# exactly to the statewide total. Every row is programName 'Food
# Establishment' with score 0 and scoreDisplay '' (no score of any kind:
# score_scale 'none', risk comes from the scraped observations), the date
# is MM/DD/YYYY (_parse_mhd_record handles it) and the detail page MUST be
# fetched under the inspection's own district slug (the statewide path
# serves the welcome page) — which one jurisdiction per district gives for
# free. The portal ran a week behind on 2026-11-02, hence lag_days 14.
# Fairfax County is NOT on the portal (its district link goes to
# fairfaxcounty.gov; it is DineScores' own 'fairfax' source); 'Lord Fairfax'
# is the Winchester-area district, not Fairfax County. ~1,000 inspections
# a week statewide, every one a detail page, so the districts run in the
# midweek group.
MHD_VA_GROUP = 'MHD metros (midweek)'


def _mhd_va_district(district, default_city, metro=''):
    """MHD_METRO_JURISDICTIONS entry for one VDH health district sub-site.
    `district` is the district's name as the portal titles it (minus
    'Health District/Department'); default_city only fills a row with no
    city (every row seen carried one); `metro` groups the Richmond, Hampton
    Roads and Northern Virginia districts under one chip each."""
    return {'display_name': f'VDH {district}', 'default_city': default_city,
            'state': 'VA', 'score_scale': 'none', 'metro': metro,
            'programs': ['Food Establishment'], 'lag_days': 14,
            'group': MHD_VA_GROUP}


MHD_METRO_JURISDICTIONS = {
    # Portland, OR metro: Multnomah + Washington + Clackamas counties (official
    # OR 0-100 scores) and Clark County WA across the river (pass/fail only).
    # Note ~2-week publishing lag — recent empty windows are normal.
    'multco-eh':            {'display_name': 'Multnomah County', 'default_city': 'Portland',
                             'state': 'OR', 'metro': 'Portland', 'lag_days': 21},
    'or-washington-county': {'display_name': 'Washington County OR', 'default_city': 'Hillsboro',
                             'state': 'OR', 'metro': 'Portland', 'lag_days': 21},
    'or-clackamas-county':  {'display_name': 'Clackamas County', 'default_city': 'Oregon City',
                             'state': 'OR', 'metro': 'Portland', 'lag_days': 21},
    'clarkcountywa':        {'display_name': 'Clark County WA', 'default_city': 'Vancouver',
                             'state': 'WA', 'score_scale': 'none', 'metro': 'Portland', 'lag_days': 21},
    # Colorado Front Range (demerit points, lower better)
    'epcph':                {'display_name': 'El Paso County CO', 'default_city': 'Colorado Springs',
                             'state': 'CO', 'score_scale': 'demerit', 'metro': ''},
    'larimer-county-health': {'display_name': 'Larimer County', 'default_city': 'Fort Collins',
                              'state': 'CO', 'score_scale': 'demerit', 'metro': ''},
    'weldcounty':           {'display_name': 'Weld County', 'default_city': 'Greeley',
                             'state': 'CO', 'score_scale': 'demerit', 'metro': ''},
    'adcogoveh':            {'display_name': 'Adams County', 'default_city': 'Brighton',
                             'state': 'CO', 'score_scale': 'demerit', 'metro': ''},
    # Utah County (Provo/Orem) — demerits
    'utah-utahcounty':      {'display_name': 'Utah County', 'default_city': 'Provo',
                             'state': 'UT', 'score_scale': 'demerit', 'metro': ''},
    # Yolo County, CA (Davis/Woodland) — pass/fail only
    'yolocountyeh':         {'display_name': 'Yolo County', 'default_city': 'Woodland',
                             'state': 'CA', 'score_scale': 'none', 'metro': ''},
    # GNR Health = Gwinnett, Newton & Rockdale counties (the north-east
    # Atlanta suburbs: Lawrenceville, Duluth, Buford, Norcross, Suwanee,
    # Snellville, Lilburn, Dacula, Sugar Hill, Loganville, Conyers,
    # Covington). Official 0-100 scores (scoreDisplay A/B/C/U), ~20-50
    # inspections a day, never near the cap. Atlanta proper is NOT here:
    # Fulton, DeKalb and Cobb counties are not on this portal — the 'fulton'
    # slug is Fulton County, ILLINOIS. Keep programName 'Food' only
    # ('Tourist' = hotels, 'Pools', 'Body Art' would pass _mhd_is_food_record).
    'gwinnett':             {'display_name': 'Gwinnett County', 'default_city': 'Lawrenceville',
                             'state': 'GA', 'score_scale': '100', 'metro': '',
                             'programs': ['Food']},
    # Tennessee DOH metros — bucket mode on the statewide 'tennessee' slug
    # (see the note above the table). Each gets a metro chip because its
    # records span suburbs.
    'tennessee:nashville':  _mhd_tn_bucket('Nashville', [
        'Nashville', 'Antioch', 'Hermitage', 'Madison', 'Goodlettsville',
        'Old Hickory', 'Brentwood', 'Franklin', 'Hendersonville', 'Murfreesboro',
        'Smyrna', 'La Vergne', 'Mt. Juliet', 'Lebanon', 'Gallatin']),
    'tennessee:memphis':    _mhd_tn_bucket('Memphis', [
        'Memphis', 'Germantown', 'Collierville', 'Bartlett', 'Cordova',
        'Millington', 'Arlington', 'Lakeland']),
    'tennessee:knoxville':  _mhd_tn_bucket('Knoxville', [
        'Knoxville', 'Farragut', 'Powell', 'Maryville', 'Alcoa', 'Oak Ridge']),
    'tennessee:chattanooga': _mhd_tn_bucket('Chattanooga', [
        'Chattanooga', 'Hixson', 'East Ridge', 'Ooltewah', 'Red Bank',
        'Soddy Daisy', 'Signal Mountain']),
    # 'orange-county' (CA) is capped AND ignores the date filter, so it is not
    # a jurisdiction here at all: fetch_orange_county polls its newest 225
    # daily into a sidecar instead (see that section).
    #
    # ── 2026-11 expansion (each verified live 2026-11-03: slug answers rows,
    # detail pages parse with _mhd_parse_detail_html, score scale and
    # programName values read off the rows) ──
    # Fort Bend County Environmental Health, TX (Richmond, Katy's Fort Bend
    # side, Fulshear, Fresno, Needville, Meadows Place — unincorporated Fort
    # Bend; Sugar Land, Missouri City, Stafford and Rosenberg run their own
    # programs). Official 0-100 score (100 <-> zero observations, 72 <-> 6),
    # ~50 food inspections a week, history from 2025-03-07. programName
    # 'Food' | 'Pools' (pool rows score 0). No metro chip: it is the Houston
    # area but Houston proper is a different source.
    'fbceh':                {'display_name': 'Fort Bend County', 'default_city': 'Richmond',
                             'state': 'TX', 'score_scale': '100', 'metro': '',
                             'programs': ['Food'], 'group': 'MHD metros'},
    # Arapahoe County Public Health, CO (Aurora's southern half, Centennial,
    # Englewood, Littleton, Greenwood Village). `score` is the county's
    # risk-index demerit total (Pass 0-49, Re-Inspection Required 50-109,
    # Closed 110+), scoreDisplay '' — demerit scale, risk recomputed from the
    # detail pages. ~65 food inspections a week since 2023-01-04. programName
    # 'Retail Food' | 'Child Care' | 'Pools' | 'Body Art' (the allowlist is
    # mandatory: child care and body art pass _mhd_is_food_record). Mobile
    # units carry addressLine1 '^Mobile Unit' with no city or zip — excluded.
    'acph':                 {'display_name': 'Arapahoe County', 'default_city': 'Aurora',
                             'state': 'CO', 'score_scale': 'demerit', 'metro': '',
                             'programs': ['Retail Food'],
                             'exclude': {'addressLine1': r'^\^?\s*Mobile Unit'},
                             'group': 'MHD metros'},
    # Douglas County Health Department, CO (Castle Rock, Parker, Highlands
    # Ranch, Lone Tree, Castle Pines). Same risk-index demerit scale as
    # Arapahoe (Pass 0-49 / Re-Inspection Required 50-109 / Closed 110+; 0
    # on a fifth of rows, up to 105), ~24 food inspections a week since
    # 2022-09-07. programName 'Retail Food' | 'Child Care' | 'Recreational
    # Water' | 'Body Art'. The display name MUST carry 'CO': Omaha's source
    # is 'Douglas County Health Dept' (OMAHA_SOURCE), and a bare 'Douglas
    # County' here would merge the two counties' freshness and Jev policy.
    'dchd':                 {'display_name': 'Douglas County CO', 'default_city': 'Castle Rock',
                             'state': 'CO', 'score_scale': 'demerit', 'metro': '',
                             'programs': ['Retail Food'], 'group': 'MHD metros'},
    # Cuyahoga County Board of Health, OH: the 58 Cleveland suburbs (Parma,
    # Lakewood, Euclid, Strongsville, Shaker Heights, ...); the City of
    # Cleveland has its own department and is NOT here. No score of any kind
    # (score 0, scoreDisplay '' on every row; Ohio's form has none), so risk
    # comes from the scraped observations ('NN: 3717-1-xx <code title>.
    # **<observation>'). ~200-230 food inspections a week (35-84 a day),
    # history from 2024-11-04. programName 'Food' | 'Schools' | 'Body Art' |
    # 'Pools' | 'Animal Venue'. `state` is null on some rows (default fills
    # it). Every row is a detail page, hence the midweek group.
    'ccbh':                 {'display_name': 'Cuyahoga County', 'default_city': 'Parma',
                             'state': 'OH', 'score_scale': 'none', 'metro': 'Cleveland',
                             'programs': ['Food'], 'group': 'MHD metros (midweek)'},
    # Virginia (VDH) — the 33 district sub-sites, see _mhd_va_district above.
    # Richmond metro
    'va-richmond':          _mhd_va_district('Richmond City', 'Richmond', 'Richmond'),
    'va-henrico':           _mhd_va_district('Henrico', 'Henrico', 'Richmond'),
    'va-chesterfield':      _mhd_va_district('Chesterfield', 'Chesterfield', 'Richmond'),
    'va-chickahominy':      _mhd_va_district('Chickahominy', 'Mechanicsville', 'Richmond'),
    # Hampton Roads
    'va-virginia-beach':    _mhd_va_district('Virginia Beach', 'Virginia Beach', 'Hampton Roads'),
    'va-norfolk':           _mhd_va_district('Norfolk', 'Norfolk', 'Hampton Roads'),
    'va-chesapeake':        _mhd_va_district('Chesapeake', 'Chesapeake', 'Hampton Roads'),
    'va-portsmouth':        _mhd_va_district('Portsmouth', 'Portsmouth', 'Hampton Roads'),
    'va-hampton':           _mhd_va_district('Hampton', 'Hampton', 'Hampton Roads'),
    'va-peninsula':         _mhd_va_district('Peninsula', 'Newport News', 'Hampton Roads'),
    'va-western-tidewater': _mhd_va_district('Western Tidewater', 'Suffolk', 'Hampton Roads'),
    # Northern Virginia (Fairfax County is its own source and not on the portal)
    'va-arlington':         _mhd_va_district('Arlington', 'Arlington', 'Northern Virginia'),
    'va-alexandria':        _mhd_va_district('Alexandria', 'Alexandria', 'Northern Virginia'),
    'va-loudoun':           _mhd_va_district('Loudoun', 'Leesburg', 'Northern Virginia'),
    'va-prince-william':    _mhd_va_district('Prince William', 'Manassas', 'Northern Virginia'),
    # The other 18 districts: no metro chip
    'va-alleghany-roanoke': _mhd_va_district('Alleghany-Roanoke', 'Roanoke'),
    'va-blue-ridge':        _mhd_va_district('Blue Ridge', 'Charlottesville'),
    'va-central':           _mhd_va_district('Central Virginia', 'Lynchburg'),
    'va-central-shenandoah': _mhd_va_district('Central Shenandoah', 'Harrisonburg'),
    'va-crater':            _mhd_va_district('Crater', 'Petersburg'),
    'va-cumberland-plateau': _mhd_va_district('Cumberland Plateau', 'Tazewell'),
    'va-eastern-shore':     _mhd_va_district('Eastern Shore', 'Accomac'),
    'va-lenowisco':         _mhd_va_district('Lenowisco', 'Wise'),
    'va-lord-fairfax':      _mhd_va_district('Lord Fairfax', 'Winchester'),
    'va-mount-rogers':      _mhd_va_district('Mount Rogers', 'Bristol'),
    'va-new-river':         _mhd_va_district('New River', 'Christiansburg'),
    'va-piedmont':          _mhd_va_district('Piedmont', 'Farmville'),
    'va-pittsylvania-danville': _mhd_va_district('Pittsylvania-Danville', 'Danville'),
    'va-rappahannock':      _mhd_va_district('Rappahannock', 'Fredericksburg'),
    'va-rappahannock-rapidan': _mhd_va_district('Rappahannock-Rapidan', 'Warrenton'),
    'va-southside':         _mhd_va_district('Southside', 'South Hill'),
    'va-three-rivers':      _mhd_va_district('Three Rivers', 'Saluda'),
    'va-west-piedmont':     _mhd_va_district('West Piedmont', 'Martinsville'),
}

MHD_DEFAULT_GROUP = 'MHD metros'   # fetch group of an entry without 'group'

# CLI city slugs → MHD_METRO_JURISDICTIONS subsets
MHD_METRO_SLUG_GROUPS = {
    'portland': ['multco-eh', 'or-washington-county', 'or-clackamas-county',
                 'clarkcountywa'],
    'coloradosprings': ['epcph'],
    'larimer': ['larimer-county-health'],
    'weld': ['weldcounty'],
    'adams': ['adcogoveh'],
    'utahcounty': ['utah-utahcounty'],
    'yolo': ['yolocountyeh'],
    'gwinnett': ['gwinnett'],
    'nashville': ['tennessee:nashville'],
    'memphis': ['tennessee:memphis'],
    'knoxville': ['tennessee:knoxville'],
    'chattanooga': ['tennessee:chattanooga'],
    # convenience: all four Tennessee metros
    'tennessee': ['tennessee:nashville', 'tennessee:memphis',
                  'tennessee:knoxville', 'tennessee:chattanooga'],
    # 2026-11 expansion
    'fortbend': ['fbceh'],
    'arapahoe': ['acph'],
    'douglas': ['dchd'],
    'cuyahoga': ['ccbh'],
    'richmond': ['va-richmond', 'va-henrico', 'va-chesterfield', 'va-chickahominy'],
    'hamptonroads': ['va-virginia-beach', 'va-norfolk', 'va-chesapeake', 'va-portsmouth',
                     'va-hampton', 'va-peninsula', 'va-western-tidewater'],
    'nova': ['va-arlington', 'va-alexandria', 'va-loudoun', 'va-prince-william'],
    # convenience: every VDH district
    'virginia': [k for k in MHD_METRO_JURISDICTIONS if k.startswith('va-')],
}

CHROME_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'

# The MyHealthDepartment portal answers a flat 403 to every GitHub-hosted
# runner type (Linux, macOS and Windows egress ranges alike — probed
# 2026-09-07), and the Southern Nevada Health District API has failed from
# runners too, so the weekly refresh can only reach those metros through an
# unblocked egress. Set PORTAL_PROXY_URL (http://user:pass@host:port — a
# residential/ISP proxy, locally or as the repo secret of the same name) and
# the MHD and SNHD requests go through it; empty means direct.
PORTAL_PROXY_URL = os.environ.get('PORTAL_PROXY_URL', '').strip()

# Alternative to the proxy above: a fetch relay (see workers/portal-relay).
# A Worker cannot serve HTTP CONNECT, so it is not a `proxies` entry — the
# target URL rides in a query parameter and the Worker fetches it. Set
# PORTAL_RELAY_URL (the deployed Worker) and PORTAL_RELAY_TOKEN (the shared
# secret it checks).
#
# Mind what the portal actually blocks on (measured 2026-09-15, one host,
# same minute): the egress IP AND the client's TLS fingerprint, independently.
# From an IP where Python `requests` and `curl` both get 200, Node's undici
# gets 403 regardless of headers. A relay therefore only helps if its own
# client is accepted, which for a Cloudflare Worker is unverified. Running
# this pipeline's Python from any non-GitHub host is the proven path.
PORTAL_RELAY_URL = os.environ.get('PORTAL_RELAY_URL', '').strip()
PORTAL_RELAY_TOKEN = os.environ.get('PORTAL_RELAY_TOKEN', '').strip()

# The relay's credential. A static PORTAL_RELAY_TOKEN works anywhere; inside
# GitHub Actions no secret is needed at all: with `id-token: write` the job can
# mint a short-lived OIDC token (GitHub signs it; the relay checks the
# signature and that `repository` is this repo) and the pipeline re-mints it
# before it expires, so a two-hour run never carries a stale credential.
RELAY_OIDC_AUDIENCE = 'dinescores-portal-relay'
_RELAY_OIDC = {'token': '', 'exp': 0.0, 'warned': False}
_RELAY_OIDC_LOCK = threading.Lock()


def _relay_token():
    """Credential to present to the relay ('' = none available)."""
    if PORTAL_RELAY_TOKEN:
        return PORTAL_RELAY_TOKEN
    req_url = os.environ.get('ACTIONS_ID_TOKEN_REQUEST_URL', '').strip()
    req_tok = os.environ.get('ACTIONS_ID_TOKEN_REQUEST_TOKEN', '').strip()
    if not (req_url and req_tok):
        return ''
    with _RELAY_OIDC_LOCK:
        if _RELAY_OIDC['token'] and time.time() < _RELAY_OIDC['exp'] - 90:
            return _RELAY_OIDC['token']
        try:
            r = requests.get(req_url, params={'audience': RELAY_OIDC_AUDIENCE},
                             headers={'Authorization': f'bearer {req_tok}',
                                      'Accept': 'application/json; api-version=2.0'},
                             timeout=30)
            r.raise_for_status()
            token = (r.json().get('value') or '').strip()
            exp = time.time() + 240
            try:   # read the token's own expiry; GitHub's are short-lived
                payload = token.split('.')[1]
                payload += '=' * (-len(payload) % 4)
                exp = float(json.loads(base64.urlsafe_b64decode(payload)).get('exp') or exp)
            except Exception:
                pass
            _RELAY_OIDC.update(token=token, exp=exp)
        except Exception as e:
            if not _RELAY_OIDC['warned']:
                log.warning(f"Relay: could not mint a GitHub OIDC token ({e}); "
                            f"portal requests go direct")
                _RELAY_OIDC['warned'] = True
            _RELAY_OIDC.update(token='', exp=0.0)
        return _RELAY_OIDC['token']


def _portal_proxies():
    """requests `proxies` mapping for the IP-sensitive portals (None = direct)."""
    if not PORTAL_PROXY_URL:
        return None
    return {'http': PORTAL_PROXY_URL, 'https': PORTAL_PROXY_URL}


def _relay(url, params=None):
    """Rewrite a portal request to go through the relay Worker.

    Returns (url, params, extra_headers); without a relay configured the
    inputs come back untouched. Query params are folded into the target
    before encoding, since anything left in `params` would land on the
    Worker's own URL instead of the portal's.
    """
    if not PORTAL_RELAY_URL or url.startswith(PORTAL_RELAY_URL):
        return url, params, {}
    token = _relay_token()
    if not token:
        # No credential to present: the relay would refuse, so go direct
        # (which fails exactly as it does today when the egress is blocked).
        return url, params, {}
    if params:
        url = f"{url}{'&' if '?' in url else '?'}{urlencode(params, doseq=True)}"
    sep = '&' if '?' in PORTAL_RELAY_URL else '?'
    relayed = f"{PORTAL_RELAY_URL}{sep}url={quote(url, safe='')}"
    return relayed, None, {'X-Relay-Token': token}


class _PortalSession(requests.Session):
    """Session that routes portal requests through the relay Worker.

    The portals' own status codes pass through untouched, so the 403/429
    retry ladders and the detail-ban cooldowns keep working as written.
    """

    def request(self, method, url, **kw):
        url, kw['params'], extra = _relay(url, kw.get('params'))
        if extra:
            kw['headers'] = {**(kw.get('headers') or {}), **extra}
        return super().request(method, url, **kw)


def _wants_portal_sources(cities):
    """True if any requested city sits behind an IP-sensitive portal."""
    portal_slugs = (set(DFW_JURISDICTIONS) | set(MHD_METRO_SLUG_GROUPS)
                    | set(MHD_METRO_JURISDICTIONS) | {'dfw', 'vegas', 'orangecounty', 'anaheim'})
    return bool(portal_slugs & set(cities or ()))


def _mhd_metro_keys(cities):
    """MHD_METRO_JURISDICTIONS keys a --cities list asks for, in order and
    without duplicates: through the CLI slugs in MHD_METRO_SLUG_GROUPS and
    as raw table keys. Keys may be pseudo-jurisdictions ('tennessee:nashville')
    and one key can be reached through several slugs ('nashville' and
    'tennessee'), hence the dedupe."""
    requested = list(cities or ())
    wanted = set(requested)
    keys = []
    for city_slug, table_keys in MHD_METRO_SLUG_GROUPS.items():
        if city_slug in wanted:
            for k in table_keys:
                if k not in keys:
                    keys.append(k)
    for k in requested:
        if k in MHD_METRO_JURISDICTIONS and k not in keys:
            keys.append(k)
    return keys


def _mhd_metro_groups(keys):
    """The fetch groups the requested MHD_METRO_JURISDICTIONS keys run in:
    {group name: {key: config}}, one group per distinct config 'group'
    (MHD_DEFAULT_GROUP when unset), groups and keys in first-seen order.
    Each group is its own report row and freshness key, so the heavy
    jurisdictions ('MHD metros (midweek)') can run on their own cron."""
    groups = {}
    for k in keys:
        config = MHD_METRO_JURISDICTIONS[k]
        groups.setdefault(str(config.get('group') or MHD_DEFAULT_GROUP), {})[k] = config
    return groups


def sane_inspection_date(date_str):
    """
    Validate an ISO inspection date (YYYY-MM-DD prefix). Source data contains
    placeholder dates (NYC uses 1900-01-01 for "not yet inspected") and
    data-entry typos years in the future (e.g. an SF record dated 2031).
    Returns the normalized YYYY-MM-DD string, or None if implausible.
    """
    s = str(date_str or '')[:10]
    try:
        d = datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        return None
    # One day of slack covers a local-time run reading a UTC-dated source;
    # anything further out is a typo (Delaware publishes rows months ahead,
    # and a "next week" inspection was shipping as a restaurant's latest).
    if d.year < 2000 or d > datetime.now() + timedelta(days=1):
        return None
    return s


def _socrata_headers():
    """Optional Socrata app token (higher rate limits). Set SOCRATA_APP_TOKEN env var."""
    token = os.environ.get('SOCRATA_APP_TOKEN', '').strip()
    return {'X-App-Token': token} if token else {}


SOCRATA_RETRIES = 3
SOCRATA_RETRY_STATUSES = (429, 500, 502, 503, 504)


def _socrata_fetch_pages(base_url, params, limit):
    """Paginate through a Socrata endpoint, returning all rows up to limit.

    Transient failures (read timeouts, connection resets, 429/5xx, a
    non-JSON error page) are retried with backoff: the state portals stall
    for a minute now and then, and a single failed page used to abandon
    the whole source for the week. Auth/4xx failures raise immediately.
    """
    headers = _socrata_headers()
    dataset = base_url.split('/')[-1].split('.')[0]
    all_rows = []
    while True:
        batch = None
        for attempt in range(1, SOCRATA_RETRIES + 1):
            try:
                r = requests.get(base_url, params=params, headers=headers, timeout=90)
                r.raise_for_status()
                batch = r.json()
                break
            except (requests.RequestException, ValueError) as e:
                status = getattr(getattr(e, 'response', None), 'status_code', None)
                transient = (status is None or status in SOCRATA_RETRY_STATUSES)
                if not transient or attempt == SOCRATA_RETRIES or _past_deadline():
                    raise
                wait = 10 * attempt
                log.warning(f"  {dataset}: {e} — retrying in {wait}s "
                            f"({attempt}/{SOCRATA_RETRIES - 1})")
                time.sleep(wait)
        if not batch:
            break
        all_rows.extend(batch)
        log.info(f"  {base_url.split('/')[-1].split('.')[0]}: fetched {len(all_rows)} rows so far...")
        if len(batch) < params['$limit']:
            break
        params['$offset'] += params['$limit']
        time.sleep(0.3)
        if limit and len(all_rows) >= limit:
            break
    return all_rows


def classify_violation(text):
    if not text:
        return ('unclassified', 'core', text)
    t = text.lower()
    for cat, patterns in VIOLATION_CATEGORIES.items():
        for pat in patterns:
            if re.search(pat, t):
                return (cat, SEVERITY_MAP[cat], text)
    return ('unclassified', 'core', text)

def calc_risk_score(violations):
    pv = pfv = cv = 0
    for (cat, sev, _) in violations:
        if sev == 'priority':
            pv += 1
        elif sev == 'priority_foundation':
            pfv += 1
        else:
            cv += 1
    score = 100 - (pv * 5) - (pfv * 2) - (cv * 1)
    return max(0, score), pv, pfv, cv

# ─── VETTED GRADING LOGIC (ported from Gemini edition utils/grading.ts) ──────

# Automatic-F trigger: ACTIVE pest evidence or sewage problems. A bare
# substring match (the original BAD_WORDS approach) misfires on violation
# titles like Chicago's "INSECTS, RODENTS, & ANIMALS NOT PRESENT" (cited for
# door gaps) and on preventive boilerplate, so an evidence word must precede
# the pest word within the same sentence.
PEST_EVIDENCE_RE = re.compile(
    r'(?:evidence of|live|dead|observed|found|fresh|infestation|activity of|'
    r'crawling|droppings?|excreta|feces)'
    r'[^.]{0,60}?\b(?:rats?|mice|mouse|roach(?:es)?|cockroach(?:es)?|rodents?|'
    r'vermin|ants?|maggots?|larvae?)\b'
    r'|\b(?:ants?|roach(?:es)?|maggots?)\b[^.]{0,40}?'
    r'(?:crawling|in(?:side)? (?:the |of )?(?:food|ice|soda|drink|prep|machine|container))',
    re.I)
SEWAGE_ISSUE_RE = re.compile(
    r'sewage[^.]{0,40}(?:back|overflow|leak|expos|floor|discharg)|'
    r'(?:back|overflow|leak)[^.]{0,40}sewage',
    re.I)
# The Food Code's dead-pest item names its pests as a list: "Dead or trapped
# birds, insects, rodents, and other pests shall be removed ..." (6-501.112,
# which Fort Bend County prints verbatim on every item-38 citation, Houston,
# Detroit and Delaware as the bare title, Dallas as "Dead insects, rodents
# and other pests have not been removed from the facility"). PEST_EVIDENCE_RE
# read "dead ... rodents" in it as evidence and forced F's on county-scored
# 97s (3 of 117 Fort Bend inspections, 2026-11-03). A list that names every
# kind names none — the rule jev_reader's names_vermin already applies to
# Jev's answers — so the construction is removed before the evidence match.
# Measured over the 161,604 distinct lines in the Jev caches: 147 regex F's
# go, every one this title (mostly Dallas's item 34 over dead crickets,
# beetles and flies), none is added; a finding written after the title
# ("... OBSERVED DEAD ROACHES UNDER THE SINK") keeps its F.
DEAD_PEST_TITLE_RE = re.compile(
    r'\b(?:remov(?:e|ing)\s+)?dead(?:\s*(?:or|/)\s*trapped)?\s+'
    r'(?:(?:flies|insects?|rodents?|birds?|animals?)\s*(?:,|;|/|&|\band\b|\bor\b)\s*(?:(?:and|or)\s+)?){2,}'
    r'(?:other\s+(?:pests?|animals?|vermin)|pests?\b)', re.I)


def has_active_pest_or_sewage(text):
    t = DEAD_PEST_TITLE_RE.sub(' ', text or '')
    return bool(PEST_EVIDENCE_RE.search(t) or SEWAGE_ISSUE_RE.search(t))


def calculate_vetted_grade(score, violation_text=''):
    """
    Proprietary grade assignment matching Gemini edition logic:
      A: score 90-100 AND no active pest/sewage evidence
      B: score 80-89
      C: score 70-79
      F: score < 70 OR active pest/sewage evidence
    """
    if score < 70 or has_active_pest_or_sewage(violation_text):
        return 'F'
    if score >= 90:
        return 'A'
    if score >= 80:
        return 'B'
    if score >= 70:
        return 'C'
    return 'F'


def detect_infractions(text):
    """
    Detect infraction categories from violation text.
    Returns list of category strings: pests, temp, hygiene, equipment, docs.
    Matches Gemini edition detectInfractions() logic.
    """
    lower = (text or '').lower()
    infractions = []

    if re.search(r'\b(vermin|roach(?:es)?|rodents?|rats?|mice|insects?|fly|flies|'
                 r'gnats?|pests?|ants?|maggots?)\b', lower):
        infractions.append('pests')
    if re.search(r'(temp|cool|heat|thaw|thermometer|refrigerat|hot|cold|hold)', lower):
        infractions.append('temp')
    if re.search(r'(hand|glove|hair|eat|drink|tobacco|fingernail|hygiene|wash)', lower):
        infractions.append('hygiene')
    if re.search(r'(sink|plumbing|water|equipment|warewash|surface|repair|door|wall|ceiling|floor|light|vent|clean)', lower):
        infractions.append('equipment')
    if re.search(r'(permit|sign|post|certified|manager|knowledge|certificate)', lower):
        infractions.append('docs')

    return infractions


# What the site shows for two of _paraphrase_violation's phrases. The phrase
# itself stays: it is one of the fixed options Jev picks from
# (jev_reader.SUMMARY_PHRASES), so renaming it would invalidate every cached
# answer. "Live roaches found" was shown for dead roaches too, and "Rodent
# activity detected" mostly reports droppings.
SUMMARY_DISPLAY_NAMES = {
    'Live roaches found': 'Roaches found',
    'Rodent activity detected': 'Rodents or droppings found',
}


def display_summary_text(phrase):
    """The public wording of a summary phrase (idempotent)."""
    return SUMMARY_DISPLAY_NAMES.get(phrase, phrase)


def _paraphrase_violation(text):
    """Convert technical violation text to user-friendly summary."""
    lower = (text or '').lower()

    # Structural / entry points (check first)
    if re.search(r'\b(outer opening|gap|seal|tight fitting|weather strip|door sweep)\b', lower):
        return "Gaps/Openings (Risk of entry)"
    # Critical / pests
    if re.search(r'\b(roach|cockroach)\b', lower):
        return "Live roaches found"
    if re.search(r'\b(rodent|rats?|mice|mouse|droppings|feces)\b', lower):
        return "Rodent activity detected"
    if re.search(r'\b(fly|flies|gnats?)\b', lower):
        return "Flies/Gnats present"
    if re.search(r'\b(sewage|wastewater)\b', lower):
        return "Sewage backup/issue"
    # Temp / food
    if re.search(r'(cold hold|41f)', lower):
        return "Food not kept cold enough"
    if re.search(r'(hot hold|135f)', lower):
        return "Food not kept hot enough"
    if re.search(r'\badulterated\b', lower) or re.search(r'\bspoiled\b', lower):
        return "Food spoiled/unsafe"
    if re.search(r'\bdented\b', lower):
        return "Dented/Damaged cans"
    if re.search(r'(cross contamination|separate)', lower):
        return "Raw food touching ready-to-eat"
    if re.search(r'\bthermometer\b', lower):
        return "Missing thermometers"
    # Hygiene
    if re.search(r'\b(hand|wash)\b', lower) and re.search(r'\b(sink|accessible)\b', lower):
        return "Hand sink blocked/dirty"
    if re.search(r'\b(hand|wash)\b', lower):
        return "Improper hand washing"
    if re.search(r'\b(eat|drink|tobacco)\b', lower):
        return "Worker eating/drinking near food"
    if re.search(r'\b(hair|restraint)\b', lower):
        return "Hair restraints missing"
    if re.search(r'\b(fingernail|jewelry)\b', lower):
        return "Long nails/jewelry on staff"
    # Sanitation
    if re.search(r'\b(toxic|chemical|label)\b', lower):
        return "Chemicals unsafe/unlabeled"
    if re.search(r'(contact surface|sanitiz)', lower):
        return "Dirty food contact surfaces"
    if re.search(r'(warewash|dish machine|sanitiz)', lower):
        return "Dishwasher not sanitizing"
    if re.search(r'\b(slime|mold|pink)\b', lower):
        return "Mold/Slime in ice machine or bin"
    if re.search(r'(pest control)', lower):
        return "No pest control records"
    # Facility
    if re.search(r'\b(plumbing|leak)\b', lower):
        return "Plumbing leaks"
    if re.search(r'\b(wall|ceiling|floor)\b', lower):
        return "Floors/Walls dirty or damaged"
    if re.search(r'(non-food contact)', lower):
        return "Dirty equipment surfaces"
    if re.search(r'\b(light|shield)\b', lower):
        return "Lights missing shields"
    if re.search(r'(vent|hood)', lower):
        return "Vent hood grease buildup"
    if re.search(r'\b(permit|posted)\b', lower):
        return "Permit not posted"

    # Fallback: truncate if long
    return text[:45] + '...' if len(text) > 50 else text


def summarize_violations(full_text):
    """
    Summarize violation text into structured summaries.
    Returns list of dicts with: text, verbatim, severity, category (max 5).
    Matches Gemini edition summarizeViolations() logic.
    """
    if not full_text or full_text == 'No violations recorded.':
        return []

    # Split by ||| (pipeline separator) or numbered format
    if '|||' in full_text:
        raw_items = full_text.split('|||')
    else:
        raw_items = re.split(r'(?=\d+\.\s)', full_text)

    summaries = []
    for item in raw_items:
        clean_raw = re.sub(r'^\d+\.\s*', '', item).strip()
        if len(clean_raw) < 5:
            continue

        lower = clean_raw.lower()

        severity = 'INFO'
        category = 'Maintenance'

        # Level 0: Conducive conditions (structural, not active)
        if re.search(r'(outer opening|gap|seal|tight fitting|weather strip|door sweep|threshold)', lower):
            severity = 'INFO'
            category = 'Structural Risk'
        # Level 1: Active hazards (critical)
        elif re.search(r'\b(vermin|roach|rodent|rats?|mice|sewage|toxic|adulterated|spoiled|pink mold|slime|droppings|feces|live|dead)\b', lower) or re.search(r'cross contaminat', lower):
            severity = 'CRITICAL'
            category = 'Active Hazard'
        elif re.search(r'\b(pest|fly|flies|gnat|insect)\b', lower):
            severity = 'CRITICAL'
            category = 'Active Hazard'
        # Level 2: Risk factors (warning)
        elif re.search(r'\b(warm|cool|temp|thermometer|refrigerat|thaw|dented|damaged)\b', lower):
            severity = 'WARNING'
            category = 'Risk Factor'
        elif re.search(r'(hand|wash|hygiene|glove|touch|eat|drink|tobacco)', lower):
            severity = 'WARNING'
            category = 'Risk Factor'
        elif re.search(r'(sanitiz|bleach|quat|dish machine|warewash)', lower):
            severity = 'WARNING'
            category = 'Risk Factor'
        # Level 3: Maintenance (info)
        elif re.search(r'(clean|dust|grease|debris|trash)', lower):
            severity = 'INFO'
            category = 'Maintenance'
        elif re.search(r'(wall|floor|ceiling|door|plumbing|sink|repair|light|shield|surface|equipment)', lower):
            severity = 'INFO'
            category = 'Maintenance'
        elif re.search(r'(sign|permit|certificate|manager|knowledge|post)', lower):
            severity = 'INFO'
            category = 'Documentation'

        summaries.append({
            'text': display_summary_text(_paraphrase_violation(clean_raw)),
            'verbatim': clean_raw,
            'severity': severity,
            'category': category,
        })

    # Sort: CRITICAL first, then WARNING, then INFO
    severity_order = {'CRITICAL': 3, 'WARNING': 2, 'INFO': 1}
    summaries.sort(key=lambda s: severity_order.get(s['severity'], 0), reverse=True)

    # Remove duplicates by summarized text
    seen_texts = set()
    unique = []
    for s in summaries:
        if s['text'] not in seen_texts:
            seen_texts.add(s['text'])
            unique.append(s)

    return unique[:5]


def compute_weighted_score(inspections_sorted):
    """
    Calculate weighted score from up to 3 most recent inspections.
    Weights: 60% latest, 30% second, 10% third.
    Matches Gemini edition groupInspections() logic.
    """
    weights = [0.6, 0.3, 0.1]
    recent = inspections_sorted[:3]

    total_score = 0.0
    total_weight = 0.0
    for idx, insp in enumerate(recent):
        w = weights[idx]
        score = insp.get('risk_score', 0) or 0
        total_score += score * w
        total_weight += w

    if total_weight > 0:
        return round(total_score / total_weight)
    return inspections_sorted[0].get('risk_score', 0) if inspections_sorted else 0


def build_violation_text(violations):
    """Build a combined violation text string from the violations array for grading."""
    if not violations:
        return ''
    parts = []
    for v in violations:
        if isinstance(v, (list, tuple)) and len(v) >= 3:
            parts.append(str(v[2]))
        elif isinstance(v, dict):
            parts.append(v.get('description', ''))
    return '|||'.join(p for p in parts if p)


# ─── COORDINATE SANITY ───────────────────────────────────────────────────────

# Sources and geocoders occasionally emit garbage coordinates (a Texas
# restaurant in the Pacific off Mexico). Wrong coordinates are worse than
# none: both merge paths preserve previously-known good coordinates, and a
# no-coord record simply stays off the map instead of appearing in the ocean.
CONUS_BOUNDS = (24.3, 49.5, -125.5, -66.5)  # (south, north, west, east)
STATE_BOUNDS = {  # generous per-state boxes for the states we cover
    'NY': (40.45, 45.05, -79.85, -71.75),
    'TX': (25.75, 36.55, -106.70, -93.45),
    'WA': (45.50, 49.05, -124.90, -116.85),
    'NV': (34.95, 42.05, -120.10, -113.95),
    'NC': (33.75, 36.65, -84.40, -75.35),
    'CA': (32.45, 42.05, -124.55, -114.05),
    'MA': (41.15, 42.95, -73.60, -69.85),
    'IL': (36.90, 42.55, -91.60, -87.00),
    'DC': (38.75, 39.05, -77.15, -76.85),
    'FL': (24.35, 31.05, -87.70, -79.95),
    # 2026-11 expansion states (Georgia, Louisiana, Arizona, Virginia, Ohio,
    # Colorado). Mirrored in src/grading.js — change both together.
    'GA': (30.35, 35.00, -85.65, -80.80),
    'LA': (28.90, 33.05, -94.05, -88.80),
    'AZ': (31.30, 37.00, -114.85, -109.00),
    'VA': (36.50, 39.50, -83.70, -75.20),
    'OH': (38.40, 42.00, -84.85, -80.50),
    'CO': (36.95, 41.05, -109.10, -102.00),
}
COORD_MARGIN = 0.6  # ° of slack around state boxes (metro suburbs, borders)


def coords_plausible(lat, lng, state=None):
    """True unless the point is outside the continental US, or the record
    names a covered state and the point is far outside it."""
    s, n, w, e = CONUS_BOUNDS
    if not (s <= lat <= n and w <= lng <= e):
        return False
    st = (state or '').strip().upper()
    if st == 'TEXAS':
        st = 'TX'
    box = STATE_BOUNDS.get(st)
    if not box:
        return True
    bs, bn, bw, be = box
    m = COORD_MARGIN
    return bs - m <= lat <= bn + m and bw - m <= lng <= be + m


def drop_implausible_coords(records):
    """Null out obviously-wrong coordinates so they geocode fresh next run
    instead of shipping to the map."""
    dropped = 0
    for r in records:
        lat, lng = r.get('latitude'), r.get('longitude')
        if not lat or not lng:
            continue
        if not coords_plausible(float(lat), float(lng), r.get('state')):
            log.warning(f"Dropping implausible coords ({lat}, {lng}) for "
                        f"{r.get('name')} ({r.get('city')}, {r.get('state')})")
            r['latitude'] = None
            r['longitude'] = None
            dropped += 1
    if dropped:
        log.info(f"Coordinate sanity: dropped {dropped} implausible pairs")


# ─── GEOCODING ───────────────────────────────────────────────────────────────

_GEOCODE_CACHE = {}
_NOMINATIM_LOCK = threading.Lock()   # Nominatim allows ONE request per second per client, across all threads
NOMINATIM_FALLBACK_CAP = int(os.environ.get('NOMINATIM_FALLBACK_CAP', '150'))   # per geocode_missing_coords call

NOMINATIM_MIN_PLACE_RANK = 26   # street level or better; never a town/suburb centroid


def geocode_address(address, city, state, nominatim_session=None):
    """Free geocoding via Nominatim (OpenStreetMap). Rate-limited to 1 req/sec."""
    key = f"{address}, {city}, {state}"
    if key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[key]
    
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': key,
            'format': 'json',
            'limit': 1,
            'addressdetails': 0,
        }
        headers = {'User-Agent': 'DineScores/1.0 (jordanbmcgowen@gmail.com)'}
        with _NOMINATIM_LOCK:   # the API pool geocodes from many threads at once
            r = requests.get(url, params=params, headers=headers, timeout=5)
            time.sleep(1.1)  # Respect Nominatim rate limit
        if r.status_code == 200:
            results = r.json()
            if results:
                # Nominatim answers an unknown house number with the street,
                # or the whole town, as the best match. A town centroid is a
                # wrong pin (it put Panama City Beach restaurants 20 km away
                # in Panama City); only street level or better is a location.
                # place_rank: 30 house/building, 26-27 street, <=25 area.
                try:
                    rank = int(results[0].get('place_rank') or 30)
                except (TypeError, ValueError):
                    rank = 30
                if rank >= NOMINATIM_MIN_PLACE_RANK:
                    lat = float(results[0]['lat'])
                    lon = float(results[0]['lon'])
                    _GEOCODE_CACHE[key] = (lat, lon)
                    return (lat, lon)
    except Exception as e:
        log.warning(f"Geocode failed for {key}: {e}")
    
    _GEOCODE_CACHE[key] = (None, None)
    return (None, None)

# ─── RESTAURANT ID GENERATION ────────────────────────────────────────────────

def make_restaurant_id(name, address, city):
    """Create a stable, unique ID for a restaurant that's consistent across runs."""
    key = f"{name.lower().strip()}|{address.lower().strip()}|{city.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()[:16]

def make_inspection_id(restaurant_id, inspection_date):
    """Create a unique inspection ID."""
    return f"{restaurant_id}_{inspection_date.replace('-','').replace('T','').replace(':','')[:8]}"

# ─── CHICAGO DATA COLLECTOR ──────────────────────────────────────────────────

def _parse_chicago_violation(part):
    """
    Parse one Chicago violation entry: "NN. TITLE - Comments: free text".
    Chicago's post-2018 form mirrors the FDA model form: violation numbers
    1-29 are foodborne-illness risk factors (priority / priority foundation),
    30+ are good retail practices (core). Within 1-29 the P-vs-PF split is
    item-specific, so the regex classifier decides between those two.
    Returns (category, severity, description) or None.
    """
    part = part.strip()
    if not part:
        return None

    num = None
    m = re.match(r'^(\d{1,2})\.\s*', part)
    if m:
        num = int(m.group(1))
        part = part[m.end():]

    if 'Comments:' in part:
        title, _, comments = part.partition('Comments:')
        title = title.strip().rstrip('-').strip()
        comments = comments.strip()
    else:
        title, comments = part.strip(), ''

    desc = f"{title}: {comments}" if (title and comments) else (comments or title)
    if len(desc) < 5:
        return None

    # Classify category from the full text; bound severity by the official number.
    cat, sev, _ = classify_violation(desc)
    if num is not None:
        if num <= 29 and sev == 'core':
            sev = 'priority_foundation'
        elif num >= 30:
            sev = 'core'
    return (cat, sev, desc[:500])


def fetch_chicago(since_date=None, limit=50000):
    """Fetch Chicago food inspections. Restaurants only, ordered by date desc."""
    log.info(f"Fetching Chicago data (since={since_date}, limit={limit})")

    base_url = "https://data.cityofchicago.org/resource/4ijn-s7e5.json"

    where_clauses = ["facility_type='Restaurant'"]
    if since_date:
        where_clauses.append(f"inspection_date > '{since_date}'")

    params = {
        '$where': ' AND '.join(where_clauses),
        '$order': 'inspection_date DESC',
        '$limit': min(limit, 50000),
        '$offset': 0,
    }

    all_records = _socrata_fetch_pages(base_url, params, limit)
    log.info(f"Chicago: {len(all_records)} total inspection records")

    # Parse into standard format
    results = []
    for rec in all_records:
        name = rec.get('dba_name') or rec.get('aka_name') or ''
        if not name:
            continue

        insp_date = sane_inspection_date(rec.get('inspection_date'))
        if not insp_date:
            continue

        # Parse violations
        violations_text = rec.get('violations', '')
        violations = []
        if violations_text:
            for part in violations_text.split('|'):
                parsed = _parse_chicago_violation(part)
                if parsed:
                    violations.append(parsed)

        risk_score, pv, pfv, cv = calc_risk_score(violations)

        lat = rec.get('latitude')
        lon = rec.get('longitude')

        results.append({
            'name': name.title(),
            'address': rec.get('address', '').title(),
            'city': 'Chicago',
            'state': 'IL',
            'zip': rec.get('zip', ''),
            'latitude': float(lat) if lat else None,
            'longitude': float(lon) if lon else None,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [[c, s, d] for c, s, d in violations],
            'source': 'Chicago Open Data',
            'source_url': f"https://data.cityofchicago.org/resource/4ijn-s7e5.json",
            'inspection_type': rec.get('inspection_type', ''),
            'results': rec.get('results', ''),
            'source_id': rec.get('inspection_id', ''),
            'metro': '',
        })

    return results

# ─── NYC DATA COLLECTOR ──────────────────────────────────────────────────────

def fetch_nyc(since_date=None, limit=200000):
    """Fetch NYC restaurant inspections. One row per violation — must aggregate by camis."""
    log.info(f"Fetching NYC data (since={since_date}, limit={limit})")
    
    base_url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    
    where_clauses = []
    if since_date:
        where_clauses.append(f"inspection_date > '{since_date}'")
    
    params = {
        '$order': 'camis,inspection_date DESC',
        '$limit': min(50000, limit) if limit else 50000,
        '$offset': 0,
    }
    if where_clauses:
        params['$where'] = ' AND '.join(where_clauses)

    all_rows = _socrata_fetch_pages(base_url, params, limit)
    log.info(f"NYC: {len(all_rows)} raw rows, now aggregating by restaurant...")
    
    # Aggregate: one row per (camis, inspection_date)
    inspections = defaultdict(lambda: {
        'violations': [],
        'score': None,
        'grade': None,
        'action': None,
        'inspection_type': None,
    })
    
    restaurant_meta = {}
    
    for row in all_rows:
        camis = row.get('camis', '')
        # NYC marks not-yet-inspected restaurants with a 1900-01-01 placeholder
        insp_date = sane_inspection_date(row.get('inspection_date'))
        if not insp_date:
            continue
        key = f"{camis}_{insp_date}"
        
        # Store restaurant metadata
        if camis not in restaurant_meta:
            restaurant_meta[camis] = {
                'name': row.get('dba', ''),
                'address': f"{row.get('building','')} {row.get('street','')}".strip(),
                'boro': row.get('boro', ''),
                'zip': row.get('zipcode', ''),
                'cuisine': row.get('cuisine_description', ''),
                'latitude': row.get('latitude'),
                'longitude': row.get('longitude'),
            }
        
        score = row.get('score')
        if score:
            inspections[key]['score'] = int(score) if str(score).isdigit() else None
        
        grade = row.get('grade', '')
        if grade and grade not in ('', 'Z', 'P', 'Not Yet Graded'):
            inspections[key]['grade'] = grade
        
        inspections[key]['action'] = row.get('action', '')
        inspections[key]['inspection_type'] = row.get('inspection_type', '')
        inspections[key]['camis'] = camis
        inspections[key]['inspection_date'] = insp_date
        
        violation_desc = row.get('violation_description', '')
        violation_code = row.get('violation_code', '')
        critical = row.get('critical_flag', '') == 'Critical'
        
        if violation_desc and len(violation_desc.strip()) > 5:
            cat, sev, desc = classify_violation(violation_desc)
            # NYC critical violations should at least be priority_foundation
            if critical and sev == 'core':
                sev = 'priority_foundation'
            inspections[key]['violations'].append([cat, sev, violation_desc[:500]])
    
    # Convert to standard format
    results = []
    for key, insp in inspections.items():
        camis = insp.get('camis', '')
        meta = restaurant_meta.get(camis, {})
        name = meta.get('name', '')
        if not name:
            continue
        
        violations = insp['violations']
        risk_score, pv, pfv, cv = calc_risk_score(violations)
        
        # NYC scores: lower is better (like golf). Convert to 100-scale.
        # NYC score of 0-13 = A, 14-27 = B, 28+ = C
        nyc_score = insp.get('score')
        
        lat = meta.get('latitude')
        lon = meta.get('longitude')
        
        boro = meta.get('boro', 'New York')
        
        results.append({
            'name': name.title(),
            'address': meta.get('address', '').title(),
            'city': 'New York',
            'state': 'NY',
            'zip': meta.get('zip', ''),
            'latitude': float(lat) if lat else None,
            'longitude': float(lon) if lon else None,
            'inspection_date': insp.get('inspection_date', ''),
            'original_score': nyc_score,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'source': 'NYC Open Data',
            'source_url': 'https://data.cityofnewyork.us/resource/43nn-pn8j.json',
            'inspection_type': insp.get('inspection_type', ''),
            'results': insp.get('action', ''),
            'source_id': key,
            'nyc_grade': insp.get('grade', ''),
            'cuisine': meta.get('cuisine', ''),
            'metro': '',
        })

    log.info(f"NYC: {len(results)} unique restaurant inspections")
    return results

# ─── SAN FRANCISCO DATA COLLECTOR ────────────────────────────────────────────

def fetch_sf(since_date=None, limit=30000):
    """Fetch SF health inspections from the new 2024+ dataset (tvy3-wexg)."""
    log.info(f"Fetching SF data (since={since_date}, limit={limit})")
    
    base_url = "https://data.sfgov.org/resource/tvy3-wexg.json"
    
    where_clauses = ["dba IS NOT NULL"]
    if since_date:
        where_clauses.append(f"inspection_date > '{since_date}'")
    
    params = {
        '$where': ' AND '.join(where_clauses),
        '$order': 'inspection_date DESC',
        '$limit': min(limit, 50000),
        '$offset': 0,
    }

    all_records = _socrata_fetch_pages(base_url, params, limit)
    log.info(f"SF: {len(all_records)} total inspection records")

    results = []
    for rec in all_records:
        name = rec.get('dba', '').strip()
        if not name:
            continue

        insp_date = sane_inspection_date(rec.get('inspection_date'))
        if not insp_date:
            continue

        # Parse SF violation codes and descriptions. Format is a chain of
        # blocks: "<CalCode section list> - <description>., <next section list> - ..."
        # where every section number starts with 11 (California Retail Food
        # Code 113700-114437). Split where a period+comma is followed by a
        # new section-number list, then take the text after the first " - ".
        violations = []
        violation_text = rec.get('violation_codes', '') or ''
        if violation_text:
            blocks = re.split(r'\.\s*,\s*(?=11\d{4})', violation_text)
            for block in blocks:
                _, sep, desc = block.partition(' - ')
                if not sep:
                    desc = block
                desc = re.sub(r'^[0-9,.()\[\]a-z\-\s]+(?=[A-Z])', '', desc.strip())
                if desc and len(desc) > 10:
                    violations.append(classify_violation(desc[:500]))

        # If no parsed violations but violation_count > 0, create generic entry
        violation_count = int(rec.get('violation_count', 0) or 0)
        if not violations and violation_count > 0:
            for _ in range(violation_count):
                violations.append(('unclassified', 'core', 'Violation recorded'))

        risk_score, pv, pfv, cv = calc_risk_score(violations)

        # Check facility_rating_status for pass/fail
        rating = rec.get('facility_rating_status', '')

        lat = rec.get('latitude')
        lon = rec.get('longitude')

        results.append({
            'name': name.title(),
            'address': rec.get('street_address', '').title(),
            'city': 'San Francisco',
            'state': 'CA',
            'zip': '',
            'latitude': float(lat) if lat else None,
            'longitude': float(lon) if lon else None,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': violation_count or len(violations),
            'violations': violations,
            'source': 'DataSF',
            'source_url': 'https://data.sfgov.org/resource/tvy3-wexg.json',
            'inspection_type': rec.get('inspection_type', ''),
            'results': rating,
            'source_id': f"{rec.get('permit_number','')}_{insp_date}",
            'neighborhood': rec.get('analysis_neighborhood', ''),
            'permit_type': rec.get('permit_type', ''),
            'metro': '',
        })

    return results

# ─── AUSTIN DATA COLLECTOR ───────────────────────────────────────────────────

# City suffixes seen in the Austin/Travis County dataset's address field,
# mapped to canonical city names ("Bee Caves" appears for the city of Bee Cave)
AUSTIN_AREA_CITIES = {
    'West Lake Hills': 'West Lake Hills', 'Dripping Springs': 'Dripping Springs',
    'Cedar Creek': 'Cedar Creek', 'Sunset Valley': 'Sunset Valley',
    'Cedar Park': 'Cedar Park', 'Round Rock': 'Round Rock',
    'Pflugerville': 'Pflugerville', 'Lago Vista': 'Lago Vista',
    'Lakeway': 'Lakeway', 'Bee Caves': 'Bee Cave', 'Bee Cave': 'Bee Cave',
    'Jonestown': 'Jonestown', 'Manchaca': 'Manchaca', 'Del Valle': 'Del Valle',
    'Spicewood': 'Spicewood', 'Rollingwood': 'Rollingwood', 'Leander': 'Leander',
    'Manor': 'Manor', 'Elgin': 'Elgin', 'Buda': 'Buda', 'Kyle': 'Kyle',
    'Bastrop': 'Bastrop', 'Smithville': 'Smithville', 'Creedmoor': 'Creedmoor',
    'Hutto': 'Hutto', 'Webberville': 'Webberville', 'Mustang Ridge': 'Mustang Ridge',
    'Point Venture': 'Point Venture', 'Volente': 'Volente', 'Georgetown': 'Georgetown',
    'Austin': 'Austin',
}


def fetch_austin(since_date=None, limit=100000):
    """
    Fetch Austin/Travis County food establishment inspection scores
    (datahub.austintexas.gov ecmv-9xxi). Scores are 0-100 (higher is
    better); the dataset publishes no violation text and no coordinates,
    so records are geocoded via the Census batch geocoder downstream.
    """
    log.info(f"Fetching Austin data (since={since_date}, limit={limit})")
    base_url = 'https://datahub.austintexas.gov/resource/ecmv-9xxi.json'
    where = []
    if since_date:
        where.append(f"inspection_date > '{since_date}'")
    params = {
        '$order': 'inspection_date DESC',
        '$limit': min(limit or 50000, 50000),
        '$offset': 0,
    }
    if where:
        params['$where'] = ' AND '.join(where)

    rows = _socrata_fetch_pages(base_url, params, limit)
    log.info(f"Austin: {len(rows)} inspection rows")

    # Austin's publishing runs weeks behind the inspections themselves, which
    # looks identical to a broken fetch in the weekly report. One aggregate
    # call settles which it is (verified 2026-09-15: the dataset's own newest
    # row was 2026-08-20, 26 days old, with every one of them already fetched).
    try:
        agg = requests.get(base_url, params={'$select': 'max(inspection_date) as mx'},
                           timeout=30)
        agg.raise_for_status()
        _note_source_lag('Austin', (agg.json() or [{}])[0].get('mx'))
    except (requests.RequestException, ValueError, IndexError, KeyError) as e:
        log.debug(f"Austin: source-max probe failed ({e})")

    # Address field embeds the city as a suffix: "1625 E 6th St Austin"
    city_suffixes = sorted(AUSTIN_AREA_CITIES, key=len, reverse=True)

    results = []
    for rec in rows:
        name = (rec.get('restaurant_name') or '').strip()
        insp_date = sane_inspection_date(rec.get('inspection_date'))
        if not name or not insp_date:
            continue
        try:
            score = int(float(rec.get('score')))
        except (TypeError, ValueError):
            continue

        raw_addr = (rec.get('address') or '').strip()
        city = 'Austin'
        street = raw_addr
        for c in city_suffixes:
            if raw_addr.lower().endswith(' ' + c.lower()):
                city = AUSTIN_AREA_CITIES[c]
                street = raw_addr[:-(len(c) + 1)].strip()
                break

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': street.title() if street.isupper() else street,
            'city': city,
            'state': 'TX',
            'zip': (rec.get('zip_code') or '').split('-')[0],
            'latitude': None,
            'longitude': None,
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': rec.get('process_description', ''),
            'results': '',
            'source': 'Austin Open Data',
            'source_url': 'https://datahub.austintexas.gov/resource/ecmv-9xxi.json',
            'source_id': f"atx_{rec.get('facility_id','')}_{insp_date}",
            'metro': '',
        })
    log.info(f"Austin: {len(results)} parsed inspections")
    return results


# ─── BOSTON DATA COLLECTOR ───────────────────────────────────────────────────

BOSTON_RESOURCE = '4582bec6-2b4f-4f9e-bc55-cbaa73117f4c'
# Violation levels: * = non-critical, ** = critical, *** = critical (foodborne)
BOSTON_LEVEL_SEVERITY = {'***': 'priority', '**': 'priority_foundation', '*': 'core'}


def fetch_boston(since_date=None, limit=None):
    """
    Fetch Boston food establishment inspections (data.boston.gov CKAN
    datastore, updated daily). One row per violation per inspection —
    aggregated by (license, inspection datetime) like NYC. Violation levels
    map to severity; risk score is computed from violations.
    """
    log.info(f"Fetching Boston data (since={since_date}, limit={limit})")
    base = 'https://data.boston.gov/api/3/action/datastore_search_sql'
    since = str(since_date)[:10] if since_date else '2024-01-01'

    all_rows = []
    page = 15000
    offset = 0
    while True:
        sql = (
            'SELECT businessname, licenseno, result, resultdttm, viol_level, '
            'violdesc, viol_status, comments, address, city, zip, location '
            f'FROM "{BOSTON_RESOURCE}" '
            f"WHERE resultdttm >= '{since}' "
            f'ORDER BY resultdttm DESC LIMIT {page} OFFSET {offset}'
        )
        r = requests.get(base, params={'sql': sql}, timeout=90)
        r.raise_for_status()
        payload = r.json()
        if not payload.get('success'):
            log.error(f"Boston SQL error: {str(payload.get('error'))[:200]}")
            break
        rows = payload['result']['records']
        if not rows:
            break
        all_rows.extend(rows)
        log.info(f"  Boston: fetched {len(all_rows)} rows so far...")
        if len(rows) < page:
            break
        offset += page
        if limit and len(all_rows) >= limit * 20:
            break
        time.sleep(0.3)

    log.info(f"Boston: {len(all_rows)} raw rows, aggregating by inspection...")

    inspections = {}
    for row in all_rows:
        lic = row.get('licenseno') or ''
        dttm = (row.get('resultdttm') or '')[:10]
        insp_date = sane_inspection_date(dttm)
        if not lic or not insp_date:
            continue
        key = f"{lic}_{insp_date}"
        insp = inspections.setdefault(key, {
            'name': row.get('businessname') or '',
            'address': row.get('address') or '',
            # The dataset covers City of Boston licenses only; its "city"
            # column holds neighborhoods (Dorchester, Roxbury, Allston, ...),
            # which are all part of Boston proper.
            'city': 'Boston',
            'zip': (row.get('zip') or '').strip(),
            'location': row.get('location'),
            'inspection_date': insp_date,
            'result': row.get('result') or '',
            'violations': [],
            'key': key,
        })
        violdesc = (row.get('violdesc') or '').strip()
        comments = (row.get('comments') or '').strip()
        # viol_status 'Pass' marks a previously-cited violation verified as
        # corrected — only 'Fail'/open citations count against this inspection.
        if violdesc and (row.get('viol_status') or '').strip() != 'Pass':
            desc = f"{violdesc}: {comments}" if comments else violdesc
            level = (row.get('viol_level') or '').strip()
            sev = BOSTON_LEVEL_SEVERITY.get(level)
            cat, clf_sev, _ = classify_violation(desc)
            insp['violations'].append([cat, sev or clf_sev, desc[:500]])

    results = []
    for insp in inspections.values():
        name = insp['name'].strip()
        if not name:
            continue
        risk_score, pv, pfv, cv = calc_risk_score(
            [tuple(v) for v in insp['violations']])
        lat = lon = None
        loc = insp.get('location') or ''
        m = re.search(r'\(?\s*(-?\d+\.\d+)[, ]+(-?\d+\.\d+)\s*\)?', str(loc))
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            if not (40 < lat < 44 and -73 < lon < -69):
                lat = lon = None
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': insp['address'].title() if insp['address'].isupper() else insp['address'],
            'city': insp['city'],
            'state': 'MA',
            'zip': insp['zip'],
            'latitude': lat,
            'longitude': lon,
            'inspection_date': insp['inspection_date'],
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(insp['violations']),
            'violations': insp['violations'],
            'inspection_type': '',
            'results': insp['result'],
            'source': 'Boston Open Data',
            'source_url': 'https://data.boston.gov/dataset/food-establishment-inspections',
            'source_id': f"bos_{insp['key']}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Boston: {len(results)} unique inspections")
    return results


# ─── SEATTLE / KING COUNTY DATA COLLECTOR ────────────────────────────────────

# business_id -> address as stored for the restaurant before the county
# republished its feed. The new dataset formats addresses differently
# ("17171 Bothell Way Ne, A207" vs the old "17171 Bothell Way Ne Ste A207"),
# and make_restaurant_id() hashes name+address+city, so without this map
# every re-inspected restaurant would resurface as a coordinate-less
# duplicate next to its old record. Built once by matching the two feeds on
# name, city and house number; businesses absent from it are genuinely new.
KC_ADDRESS_MAP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'data', 'king_county_address_map.json')
_KC_ADDRESS_MAP = None


def _kc_address_map():
    global _KC_ADDRESS_MAP
    if _KC_ADDRESS_MAP is None:
        try:
            with open(KC_ADDRESS_MAP_PATH) as f:
                _KC_ADDRESS_MAP = json.load(f)
            log.info(f"  Seattle: {len(_KC_ADDRESS_MAP)} King County businesses mapped to "
                     f"their stored addresses")
        except FileNotFoundError:
            log.warning(f"  Seattle: {KC_ADDRESS_MAP_PATH} missing — restaurant ids for "
                        f"re-inspected businesses will NOT match existing records")
            _KC_ADDRESS_MAP = {}
    return _KC_ADDRESS_MAP


def fetch_seattle(since_date=None, limit=None):
    """
    Fetch King County (Seattle, Bellevue, Kirkland, ...) food inspections
    (data.kingcounty.gov r878-4sxa). One row per violation — aggregated by
    inspection_serial_num. inspection_score is violation POINTS (0 = clean,
    higher = worse); risk_score = 100 - points. RED violations are food
    safety (priority), BLUE are maintenance (core).
    NOTE: the county republished the feed under a new dataset id in 2026
    (the old f29f-zza5 now answers 403 "must be logged in"; its last row
    was 2025-11-26). The new dataset carries the same columns minus
    coordinates, so new restaurants are geocoded downstream.
    """
    log.info(f"Fetching Seattle/King County data (since={since_date}, limit={limit})")
    base_url = 'https://data.kingcounty.gov/resource/r878-4sxa.json'
    where = []
    if since_date:
        where.append(f"inspection_date > '{since_date}'")
    params = {
        '$order': 'inspection_date DESC, inspection_serial_num',
        '$limit': 50000,
        '$offset': 0,
    }
    if where:
        params['$where'] = ' AND '.join(where)

    rows = _socrata_fetch_pages(base_url, params, (limit or 0) * 20 or None)
    log.info(f"Seattle: {len(rows)} raw rows, aggregating by inspection...")
    address_map = _kc_address_map()

    inspections = {}
    for row in rows:
        serial = row.get('inspection_serial_num') or ''
        insp_date = sane_inspection_date(row.get('inspection_date'))
        if not serial or not insp_date:
            continue
        insp = inspections.setdefault(serial, {
            'name': row.get('name') or row.get('inspection_business_name') or '',
            # Stored address for a known business keeps its restaurant id
            # stable across the county's address reformatting.
            'address': (address_map.get(str(row.get('business_id') or ''))
                        or (row.get('address') or '').strip()),
            'city': (row.get('city') or 'Seattle').strip().title(),
            'zip': (row.get('zip_code') or '').strip(),
            'latitude': row.get('latitude'),
            'longitude': row.get('longitude'),
            'inspection_date': insp_date,
            'inspection_type': row.get('inspection_type') or '',
            'result': row.get('inspection_result') or '',
            'points': row.get('inspection_score'),
            'violations': [],
            'serial': serial,
        })
        desc = (row.get('violation_description') or '').strip()
        if desc and len(desc) > 5:
            desc = re.sub(r'^\d+\s*-\s*', '', desc)
            vtype = (row.get('violation_type') or '').upper()
            sev = 'priority' if vtype == 'RED' else 'core'
            cat, _, _ = classify_violation(desc)
            insp['violations'].append([cat, sev, desc[:500]])

    results = []
    for insp in inspections.values():
        name = insp['name'].strip()
        if not name:
            continue
        try:
            points = int(float(insp['points']))
        except (TypeError, ValueError):
            points = None
        _, pv, pfv, cv = calc_risk_score([tuple(v) for v in insp['violations']])
        if points is not None:
            risk = max(0, min(100, 100 - points))
        else:
            risk, pv, pfv, cv = calc_risk_score([tuple(v) for v in insp['violations']])
        lat = insp.get('latitude')
        lon = insp.get('longitude')
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': insp['address'].title() if insp['address'].isupper() else insp['address'],
            'city': insp['city'],
            'state': 'WA',
            'zip': insp['zip'],
            'latitude': float(lat) if lat else None,
            'longitude': float(lon) if lon else None,
            'inspection_date': insp['inspection_date'],
            'original_score': points,
            'risk_score': risk,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(insp['violations']),
            'violations': insp['violations'],
            'inspection_type': insp['inspection_type'],
            'results': insp['result'],
            'source': 'King County Open Data',
            'source_url': 'https://data.kingcounty.gov/resource/r878-4sxa.json',
            'source_id': f"kc_{insp['serial']}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Seattle/King County: {len(results)} unique inspections")
    return results


# ─── HOUSTON (Tyler healthinspections.us portal) ────────────────────────────

HOUSTON_BASE = 'https://houston-tx.healthinspections.us/media/'
HOUSTON_DELAY = 1.2
HOUSTON_MAXROWS = 500
TYLER_DETAIL_WORKERS = 2   # Tyler portals rate-limit hard — go gently
TYLER_DETAIL_DELAY = 0.7
TYLER_RETRIES = 5
# A detail fetch that fails outright must never be recorded as a clean
# inspection. This many consecutive failures means the network (not a page)
# is broken — abort the run instead of accumulating false-clean records.
TYLER_ABORT_AFTER = 15


def _tyler_request(session, method, url, **kwargs):
    """
    Request against a Tyler healthinspections.us portal with exponential
    backoff — these servers intermittently 503 under modest request rates.
    Returns the response, or None after exhausting retries.
    """
    for attempt in range(TYLER_RETRIES):
        try:
            r = session.request(method, url, timeout=90, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                if _past_deadline():
                    log.warning(f"  Tyler {r.status_code} on {url[:60]} — "
                                f"fetch deadline reached, not retrying")
                    return None
                wait = 20 * (attempt + 1)
                # San Antonio's portal answers a quarter of detail requests
                # with a 503 carrying "Retry-After: 3" and serves the retry;
                # honour a short hint on the first attempts instead of the
                # 20 s ladder, which turned a 5-minute detail phase into 30.
                try:
                    hinted = float(r.headers.get('Retry-After', ''))
                except (TypeError, ValueError):
                    hinted = None
                if hinted is not None and 0 < hinted <= 10 and attempt < 2:
                    wait = hinted + 0.5
                    log.debug(f"  Tyler {r.status_code} on {url[:60]} — retry in {wait:.1f}s (Retry-After)")
                else:
                    log.warning(f"  Tyler {r.status_code} on {url[:60]} — retry in {wait}s "
                                f"({attempt + 1}/{TYLER_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if _past_deadline():
                log.warning(f"  Tyler error {str(e)[:80]} — "
                            f"fetch deadline reached, not retrying")
                return None
            wait = 20 * (attempt + 1)
            log.warning(f"  Tyler error {str(e)[:80]} — retry in {wait}s "
                        f"({attempt + 1}/{TYLER_RETRIES})")
            time.sleep(wait)
    return None


def _houston_session():
    s = requests.Session()
    s.headers.update({'User-Agent': CHROME_UA})
    _tyler_request(s, 'GET', HOUSTON_BASE + 'search.cfm')  # establish CF session
    return s


def _houston_search_window(session, start, end):
    """POST one date-window search; returns list of (f_id, i_id, name, address, city, zip, date, status)."""
    data = {
        'q': 's', 'e': '', 'k': '', 'r': '', 'tp': '',
        'sd_month': f'{start.month:02d}', 'sd_day': f'{start.day:02d}', 'sd_year': str(start.year),
        'sd': start.strftime('%m/%d/%Y'),
        'ed_month': f'{end.month:02d}', 'ed_day': f'{end.day:02d}', 'ed_year': str(end.year),
        'ed': end.strftime('%m/%d/%Y'),
        'z': '', 'm': '', 'maxrows': str(HOUSTON_MAXROWS), 'Submit': 'Search',
    }
    r = _tyler_request(session, 'POST', HOUSTON_BASE + 'search.cfm', data=data,
                       headers={'Referer': HOUSTON_BASE + 'search.cfm'})
    if r is None:
        raise RuntimeError('Houston search failed after retries')
    rows = []
    pattern = re.compile(
        r'href="search\.cfm\?q=d&f=([A-F0-9-]+)&i=([A-F0-9-]+)[^"]*">([^<]+)</a><br>\s*'
        r'([^<]+?)\s*</td>\s*<td[^>]*>([^<]*)</td>\s*'
        r'<td[^>]*>(\d{2}/\d{2}/\d{4})</td>\s*<td[^>]*>\s*([^<]*)</td>', re.S)
    for m in pattern.finditer(r.text):
        f_id, i_id, name, addr_line, site, date_str, status = m.groups()
        # Format: "4395 WEST SAM HOUSTON PKWY HOUSTON TX, 77041" — the
        # department covers Houston city limits, so the city token before
        # "TX" is (near-)always HOUSTON; strip it from the street greedily.
        addr = addr_line.strip()
        zm = re.search(r'\bTX,?\s*(\d{5})', addr)
        zip_code = zm.group(1) if zm else ''
        street = re.sub(r'\s+TX,?\s*\d{5}.*$', '', addr).strip()
        city = 'Houston'
        cm = re.match(r'^(.*)\s+HOUSTON$', street, re.I)
        if cm:
            street = cm.group(1).strip()
        rows.append((f_id, i_id, name.strip(), street, city,
                     zip_code, date_str, status.strip()))
    return rows


# Houston's "Inspection Results" table has two layouts: most pages list only
# the violated items, but some list the ENTIRE inspection checklist — one row
# per form item, where non-violated items carry a section-header tooltip
# ("Foodborne Illness Risk Factors and Public Health Interventions: 12") and
# an empty Status cell, while actual violations have ordinance text in the
# tooltip and "Violation" in the Status cell.
HOUSTON_ROW_RE = re.compile(
    r"ddrivetip\('(.{10,600}?)'\s*,.*?</a></td>\s*<td[^>]*>(.*?)</td>", re.S)
HOUSTON_FORM_HEADER_RE = re.compile(
    r'^(Foodborne Illness Risk Factors and Public Health Interventions|'
    r'Good Retail Practices)\s*:?\s*[\d.]*\s*$')


def _houston_fetch_detail(session, f_id, i_id, date_str):
    """
    Fetch one inspection detail page. Returns the list of violation texts
    (empty for a genuinely clean page), or None when the fetch itself failed —
    callers must not record None as a clean inspection.
    """
    url = (f'{HOUSTON_BASE}search.cfm?q=d&f={f_id}&i={i_id}'
           f'&sd={date_str}&ed={date_str}&z=&m=&maxrows=10&e=&tp=')
    try:
        r = _tyler_request(session, 'GET', url,
                           headers={'Referer': HOUSTON_BASE + 'search.cfm'})
        if r is None:
            return None
        seg = r.text
        i = seg.find('Inspection Results')
        if i >= 0:
            seg = seg[i:]
        out, fallback = [], []
        for tip, status in HOUSTON_ROW_RE.findall(seg):
            text = html_lib.unescape(tip).replace("\\'", "'")
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) <= 10 or HOUSTON_FORM_HEADER_RE.match(text):
                continue
            status_txt = re.sub(r'<[^>]+>', ' ', status)
            status_txt = re.sub(r'\s+', ' ', status_txt).strip()
            if 'Violation' in status_txt:
                out.append(text[:500])
            fallback.append(text[:500])
        # If no row carried a "Violation" status the page is either clean
        # (fallback is empty too) or a template variant without status
        # cells — fall back to the non-header tooltips.
        return out if out else fallback
    except requests.RequestException:
        return None


def fetch_houston(since_date=None, until_date=None, limit=None, fetch_violations=True):
    """
    Fetch Houston food inspections from the city's Tyler Technologies portal
    (houston-tx.healthinspections.us). Date-windowed searches (bisected when
    the 500-row cap is hit); each inspection's detail page carries the full
    ordinance text of every violation in its tooltip markup. No numeric
    score is published — risk is computed from violations.
    """
    log.info(f"Fetching Houston data (since={since_date}, limit={limit})")
    session = _houston_session()

    start = datetime.strptime(str(since_date)[:10], '%Y-%m-%d') if since_date \
        else datetime(datetime.now().year, 1, 1)
    end = datetime.now()
    if until_date:
        end = min(end, datetime.strptime(str(until_date)[:10], '%Y-%m-%d'))

    # 3-day windows, bisected on cap
    stack = []
    cur = start
    while cur <= end:
        wend = min(cur + timedelta(days=2), end)
        stack.append((cur, wend))
        cur = wend + timedelta(days=1)
    stack.reverse()

    seen = set()
    raw = []
    while stack:
        ws, we = stack.pop()
        if _past_deadline():
            log.warning(f"  Houston: fetch deadline reached — stopping search "
                        f"with {len(raw)} rows")
            break
        rows = _houston_search_window(session, ws, we)
        if len(rows) >= HOUSTON_MAXROWS and ws < we:
            mid = ws + (we - ws) / 2
            stack.append((ws, mid))
            stack.append((mid + timedelta(days=1), we))
            continue
        if len(rows) >= HOUSTON_MAXROWS:
            log.warning(f"  Houston {ws:%m/%d}: single day hit the 500-row cap")
        new = 0
        for row in rows:
            key = (row[0], row[1])
            if key not in seen:
                seen.add(key)
                raw.append(row)
                new += 1
        log.info(f"  Houston {ws:%m/%d}..{we:%m/%d}: {len(rows)} rows ({new} new), total {len(raw)}")
        if limit and len(raw) >= limit:
            raw = raw[:limit]
            break
        time.sleep(HOUSTON_DELAY)

    results = []
    for f_id, i_id, name, street, city, zip_code, date_str, status in raw:
        insp_date = sane_inspection_date(datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d'))
        if not insp_date or not name:
            continue
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': street.title() if street.isupper() else street,
            'city': city or 'Houston',
            'state': 'TX',
            'zip': zip_code,
            'latitude': None,
            'longitude': None,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': 100,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': status,
            'source': 'Houston Health Dept',
            'source_url': 'https://houston-tx.healthinspections.us/media/search.cfm',
            'source_id': f'hou_{f_id}_{i_id}',
            'metro': '',
            '_hou_ids': (f_id, i_id, date_str),
        })

    if fetch_violations and results and _past_deadline():
        # Search-only records default to risk 100 with no violations — they
        # must not ship undetailed. Drop the run; the merge keeps existing
        # Houston data and a future run re-attempts it.
        log.warning(f"Houston: fetch deadline reached before the detail phase — "
                    f"dropping {len(results)} undetailed inspections")
        return []
    if fetch_violations and results:
        log.info(f"Houston: fetching violation details for {len(results)} inspections "
                 f"({TYLER_DETAIL_WORKERS} workers)...")
        local = threading.local()

        def get_session():
            if not hasattr(local, 's'):
                local.s = _houston_session()
            return local.s

        def work(rec):
            f_id, i_id, date_str = rec['_hou_ids']
            texts = _houston_fetch_detail(get_session(), f_id, i_id, date_str)
            time.sleep(TYLER_DETAIL_DELAY)
            return rec, texts

        def apply_texts(rec, texts):
            violations = [classify_violation(t) for t in texts]
            risk, pv, pfv, cv = calc_risk_score(violations)
            rec.update(risk_score=risk, priority_violations=pv,
                       priority_foundation_violations=pfv, core_violations=cv,
                       total_violations=len(violations),
                       violations=[[c, s, d] for c, s, d in violations])

        done = withv = consecutive_failures = 0
        failed = []
        with ThreadPoolExecutor(max_workers=TYLER_DETAIL_WORKERS) as pool:
            futures = [pool.submit(work, r) for r in results]
            for future in as_completed(futures):
                rec, texts = future.result()
                done += 1
                if texts is None:
                    failed.append(rec)
                    consecutive_failures += 1
                    if consecutive_failures >= TYLER_ABORT_AFTER:
                        for f in futures:
                            f.cancel()
                        raise RuntimeError(
                            f"Houston: {consecutive_failures} consecutive detail "
                            f"failures — aborting instead of recording "
                            f"false-clean inspections")
                else:
                    consecutive_failures = 0
                    if texts:
                        apply_texts(rec, texts)
                        withv += 1
                if done % 200 == 0:
                    log.info(f"  Houston details: {done}/{len(results)} ({withv} with violations)")

        if failed:
            log.warning(f"Houston: retrying {len(failed)} failed detail fetches")
            retry_session = _houston_session()
            still_failed = []
            for rec in failed:
                f_id, i_id, date_str = rec['_hou_ids']
                texts = _houston_fetch_detail(retry_session, f_id, i_id, date_str)
                time.sleep(TYLER_DETAIL_DELAY)
                if texts is None:
                    still_failed.append(rec)
                elif texts:
                    apply_texts(rec, texts)
                    withv += 1
            if still_failed:
                # An inspection whose violations could not be read must not
                # appear violation-free — drop it; the weekly refresh or a
                # later backfill picks it up again.
                log.warning(f"Houston: dropping {len(still_failed)} inspections "
                            f"with unreadable detail pages")
                drop = {id(r) for r in still_failed}
                results = [r for r in results if id(r) not in drop]
        log.info(f"Houston: {withv}/{len(results)} inspections had violations")

    for rec in results:
        rec.pop('_hou_ids', None)
    log.info(f"Houston: {len(results)} inspections")
    return results


# ─── SAN ANTONIO, TX (Tyler healthinspections.us portal, Metro Health) ──────

SA_BASE = 'https://sanantonio-tx.healthinspections.us/san%20antonio/'
SA_DELAY = 1.2            # between search requests (same pacing as Houston)
SA_PAGE_SIZE = 10         # facilities per search page (start=1,11,21,...)
SA_SEARCH_CAP = 100       # the portal never reports more than 100 facilities
# Fallback partition for a day that hits the cap: the city's 782xx range plus
# the suburbs Metro Health inspects; zips seen in results join the pool at
# run time. A facility with no zip cannot be recovered this way.
SA_ZIPS = [f'782{n:02d}' for n in range(1, 67)] + [
    '78002', '78006', '78015', '78023', '78039', '78052', '78054', '78069',
    '78073', '78101', '78108', '78109', '78112', '78124', '78148', '78150',
    '78152', '78154', '78163',
]

# One search result: facility name linked to its last inspection, then the
# street line, the "San Antonio , TX 78216" line and the last-inspection date.
SA_RESULT_RE = re.compile(
    r'<a href="estab\.cfm\?licenseID=(\d+)&(?:amp;)?inspectionID=(\d+)"><b>(.*?)</b></a>'
    r'\s*<div style="margin-bottom:10px;">\s*(.*?)<br\s*/?>\s*(.*?)\s*<div style="color:green;">'
    r'\s*Last Inspection Date:\s*<a href="[^"]*">(\d{2}/\d{2}/\d{4})</a>', re.S)
SA_MATCHED_RE = re.compile(r'<b>\s*(\d+)\s+Facilities matched')
# One "Related Reports" block per inspection on an estab.cfm page.
SA_REPORT_SPLIT_RE = re.compile(
    r'<div style="border:1px solid #003399;width:95%;margin-bottom:10px;">')
SA_REPORT_DATE_RE = re.compile(r'<b>Date:</b>\s*(\d{2}/\d{2}/\d{4})')
SA_REPORT_SCORE_RE = re.compile(r'<b>Score</b>\s*(\d*)')
SA_VIOLATION_DIV_RE = re.compile(
    r'<div style="background-color:#EFEFEF;padding:5px;">(.*?)</div>', re.S)


def _sa_session():
    s = requests.Session()
    s.headers.update({'User-Agent': CHROME_UA, 'Referer': SA_BASE + 'search.cfm'})
    return s


def _sa_search_url(day, start=1, zc=''):
    d = day.strftime('%m/%d/%Y')
    return (f'{SA_BASE}search.cfm?1=1&sd={d}&ed={d}&kw1=&kw2=&kw3='
            f'&rel1=L.licenseName&rel2=L.licenseName&rel3=L.licenseName'
            f'&zc={zc}&dtRng=YES&pre=similar&smoking=nosmoking&start={start}')


def _sa_clean(fragment):
    text = re.sub(r'<[^>]+>', ' ', fragment or '')
    return ' '.join(html_lib.unescape(text).split())


def _sa_parse_search(html):
    """
    Parse one search page. Returns (matched, rows): matched is the page's
    "N Facilities matched" count (None when the marker is missing, i.e. the
    body is not a results page), rows are (license_id, inspection_id, name,
    street, zip, city_token, date_str) for the facilities on this page.
    """
    m = SA_MATCHED_RE.search(html)
    matched = int(m.group(1)) if m else None
    rows = []
    for lic, insp, name, street, cityline, date_str in SA_RESULT_RE.findall(html):
        cityline = _sa_clean(cityline)
        cm = re.match(r'^(.*?)\s*,\s*([A-Z]{2})\b\s*(\d{5})?', cityline)
        city_tok = cm.group(1).strip() if cm else cityline
        zip_code = (cm.group(3) or '') if cm else ''
        rows.append((lic, insp, _sa_clean(name), _sa_clean(street), zip_code,
                     city_tok, date_str))
    return matched, rows


def _sa_search_day(session, day, zc=''):
    """
    Page through one day's search (optionally one zip). Returns (matched,
    rows): the portal's facility count for the query (capped at
    SA_SEARCH_CAP) and the facilities actually listed. Raises RuntimeError
    when a page cannot be fetched or is not a results page.
    """
    rows, start, matched = [], 1, None
    while True:
        url = _sa_search_url(day, start, zc)
        r = _tyler_request(session, 'GET', url)
        page_matched = page_rows = None
        if r is not None:
            page_matched, page_rows = _sa_parse_search(r.text)
            if page_matched is None:
                # A 200 that is not a results page: re-read it once.
                time.sleep(SA_DELAY)
                r = _tyler_request(session, 'GET', url)
                if r is not None:
                    page_matched, page_rows = _sa_parse_search(r.text)
        if r is None or page_matched is None:
            raise RuntimeError(f'San Antonio search failed ({day:%m/%d/%Y} '
                               f'zc={zc or "-"} start={start})')
        if matched is None:
            matched = page_matched
        rows.extend(page_rows)
        if not page_rows or len(rows) >= matched or len(rows) >= SA_SEARCH_CAP:
            break
        if _past_deadline():
            break
        start += SA_PAGE_SIZE
        time.sleep(SA_DELAY)
    return matched, rows


def _sa_search_day_partitioned(session, day, zip_pool, stats):
    """
    All facilities whose LAST inspection fell on `day`. A day that reports
    the 100-facility cap is re-queried per zip and the per-zip results are
    unioned; the log says whether the partition recovered the cap.
    """
    matched, rows = _sa_search_day(session, day)
    stats['days'] += 1
    if matched < SA_SEARCH_CAP:
        return rows
    stats['capped_days'] += 1
    zips = sorted(set(zip_pool) | {r[4] for r in rows if r[4]})
    log.warning(f"  San Antonio {day:%m/%d}: hit the {SA_SEARCH_CAP}-facility cap — "
                f"re-querying per zip ({len(zips)} zips)")
    union = {(r[0], r[1]): r for r in rows}
    zip_capped = 0
    for zc in zips:
        if _past_deadline():
            log.warning(f"  San Antonio {day:%m/%d}: fetch deadline reached mid-partition")
            break
        time.sleep(SA_DELAY)
        zm, zrows = _sa_search_day(session, day, zc)
        if zm >= SA_SEARCH_CAP:
            zip_capped += 1
            log.warning(f"  San Antonio {day:%m/%d} zip {zc}: itself at the cap")
        for r in zrows:
            union.setdefault((r[0], r[1]), r)
    if len(union) >= SA_SEARCH_CAP and not zip_capped:
        stats['recovered_days'] += 1
        log.info(f"  San Antonio {day:%m/%d}: zip partition recovered "
                 f"{len(union)} facilities (>= cap)")
    else:
        log.warning(f"  San Antonio {day:%m/%d}: zip partition yielded {len(union)} "
                    f"facilities for a >= {SA_SEARCH_CAP} day ({zip_capped} zips capped) "
                    f"— some may be missing")
    return list(union.values())


def _sa_violation_text(raw_html):
    """
    One violation block → one line, the inspector's finding first. The
    portal has three layouts: "<item> <cite> <rule text> AT INSPECTION:
    <finding> [C.O.S.: ...]", "<item> Observation: <finding> FDA Code <cite>:
    <rule>" and a bare "<item> <finding>". The leading form-item number is
    dropped; the Food Code cite and rule text follow the finding.
    """
    t = re.sub(r'<br\s*/?>', '\n', raw_html or '', flags=re.I)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = html_lib.unescape(t).strip()
    if not t:
        return ''
    t = re.sub(r'^\d{1,3}\s+', '', t)
    m = re.search(r'AT INSPECTION\s*:', t)
    if m:
        parts = [t[m.end():], t[:m.start()]]
    else:
        m = re.match(r'\s*Observation\s*:', t)
        parts = [t[m.end():] if m else t]
    text = ' — '.join(' '.join(p.split()) for p in parts if p.strip())
    return text[:500]


def _sa_parse_estab(html):
    """
    Parse an estab.cfm page into its inspection blocks, newest first:
    [{'date': 'MM/DD/YYYY', 'score': int|None, 'texts': [...]}, ...].
    Returns None when the body is not a facility page at all (an error page
    must never read as a clean inspection); [] for a facility page that
    lists no inspections.
    """
    if '<H3>Facility</H3>' not in html or 'Related Reports' not in html:
        return None
    seg = html[html.find('Related Reports'):]
    cut = seg.find('<b>NOTE:</b>')
    if cut >= 0:
        seg = seg[:cut]
    blocks = []
    for piece in SA_REPORT_SPLIT_RE.split(seg)[1:]:
        dm = SA_REPORT_DATE_RE.search(piece)
        if not dm:
            continue
        sm = SA_REPORT_SCORE_RE.search(piece)
        score = int(sm.group(1)) if sm and sm.group(1) else None
        texts = []
        for raw in SA_VIOLATION_DIV_RE.findall(piece):
            text = _sa_violation_text(raw)
            if len(text) > 5:
                texts.append(text)
        blocks.append({'date': dm.group(1), 'score': score, 'texts': texts})
    return blocks


def _sa_fetch_facility(session, license_id):
    """
    Fetch a facility's estab.cfm page (every inspection the portal keeps for
    it, with scores and violation text). Returns the parsed blocks, or None
    when the fetch failed or the body is not a facility page — callers must
    not record None as a clean inspection.
    """
    try:
        r = _tyler_request(session, 'GET', f'{SA_BASE}estab.cfm?licenseID={license_id}')
    except requests.RequestException:
        return None
    if r is None:
        return None
    return _sa_parse_estab(r.text)


def _sa_build_records(fac, blocks, since_iso, until_iso):
    """
    Records for one facility from its parsed inspection blocks, limited to
    [since_iso, until_iso]. A block whose date the search listed keeps the
    portal's inspectionID (source_id sa_<L>_<I>, estab URL with the id); a
    block the search never listed carries no id on the page and gets
    sa_<L>_<YYYYMMDD> (D1 keys inspections by restaurant+date, so the two
    never collide).
    """
    out = []
    for block in blocks:
        try:
            iso = datetime.strptime(block['date'], '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            continue
        insp_date = sane_inspection_date(iso)
        if not insp_date or insp_date < since_iso or insp_date > until_iso:
            continue
        violations = [classify_violation(t) for t in block['texts']]
        computed, pv, pfv, cv = calc_risk_score(violations)
        score = block.get('score')
        if score is not None:
            risk = max(0, min(100, score))
        else:
            risk = computed
        insp_id = fac['inspections'].get(insp_date)
        if insp_id:
            source_id = f"sa_{fac['license_id']}_{insp_id}"
            url = f"{SA_BASE}estab.cfm?licenseID={fac['license_id']}&inspectionID={insp_id}"
        else:
            source_id = f"sa_{fac['license_id']}_{insp_date.replace('-', '')}"
            url = f"{SA_BASE}estab.cfm?licenseID={fac['license_id']}"
        name, street = fac['name'], fac['street']
        out.append({
            'name': name.title() if name.isupper() else name,
            'address': street.title() if street.isupper() else street,
            'city': 'San Antonio',
            'state': 'TX',
            'zip': fac['zip'],
            'latitude': None,
            'longitude': None,
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': risk,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [[c, s, d] for c, s, d in violations],
            'inspection_type': '',
            'results': '',
            'source': 'San Antonio Metro Health',
            'source_url': url,
            'source_id': source_id,
            'metro': '',
        })
    return out


def fetch_san_antonio(since_date=None, until_date=None, limit=None, fetch_violations=True):
    """
    Fetch San Antonio food inspections from Metro Health's Tyler portal
    (sanantonio-tx.healthinspections.us). The search lists the FACILITIES
    with an inspection in the window (one row per facility, linking that
    inspection's id), 10 per page and never more than 100 per query, so
    days are searched one at a time and a capped day is re-queried per zip.
    Each facility's estab.cfm page lists its recent inspections (up to ~10,
    newest first) with Metro Health's official 0-100 score (100 minus
    demerits) and the violation text, so one detail fetch per facility
    yields every one of its inspections inside the window. Only days the search
    actually covered ship, so a bounded run never advances freshness past
    them. The official score is the risk score; it is computed from the
    violations only when a block has no score. No coordinates — records
    are Census-geocoded by the caller.
    """
    log.info(f"Fetching San Antonio data (since={since_date}, limit={limit})")
    session = _sa_session()

    start = datetime.strptime(str(since_date)[:10], '%Y-%m-%d') if since_date \
        else datetime(datetime.now().year, 1, 1)
    end = datetime.now()
    if until_date:
        end = min(end, datetime.strptime(str(until_date)[:10], '%Y-%m-%d'))
    since_iso, until_iso = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

    stats = {'days': 0, 'capped_days': 0, 'recovered_days': 0}
    zip_pool = set(SA_ZIPS)
    seen = set()
    raw = []
    newest = ''
    searched_through = None   # last day whose search completed
    day = start
    while day.date() <= end.date():
        if _past_deadline():
            log.warning(f"  San Antonio: fetch deadline reached — stopping search "
                        f"with {len(raw)} facilities")
            break
        rows = _sa_search_day_partitioned(session, day, zip_pool, stats)
        searched_through = day
        new = 0
        for row in rows:
            key = (row[0], row[1])
            if key in seen:
                continue
            seen.add(key)
            raw.append(row)
            new += 1
            if row[4]:
                zip_pool.add(row[4])
            try:
                iso = datetime.strptime(row[6], '%m/%d/%Y').strftime('%Y-%m-%d')
            except ValueError:
                iso = ''
            newest = max(newest, iso)
        log.info(f"  San Antonio {day:%m/%d}: {len(rows)} facilities ({new} new), "
                 f"total {len(raw)}")
        if limit and len(raw) >= limit:
            raw = raw[:limit]
            break
        day += timedelta(days=1)
        time.sleep(SA_DELAY)
    if stats['capped_days']:
        log.info(f"San Antonio: {stats['capped_days']} of {stats['days']} days hit the "
                 f"{SA_SEARCH_CAP} cap; zip partition recovered {stats['recovered_days']}")
    if searched_through is None:
        return []
    complete = searched_through.date() >= end.date()
    if not complete:
        # Ship nothing from days the search never reached: their facilities
        # would otherwise post-date the covered window and move the weekly
        # lookback past days nobody searched.
        until_iso = min(until_iso, searched_through.strftime('%Y-%m-%d'))

    # One entry per facility; the search gives the id of each inspection it
    # listed (one row per facility per day searched).
    facilities = {}
    other_cities = defaultdict(int)
    for lic, insp, name, street, zip_code, city_tok, date_str in raw:
        try:
            iso = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            continue
        if not name:
            continue
        if city_tok and city_tok.upper() != 'SAN ANTONIO':
            other_cities[city_tok] += 1
        fac = facilities.setdefault(lic, {
            'license_id': lic, 'name': name, 'street': street, 'zip': zip_code,
            'inspections': {}, 'newest_hit': iso,
        })
        fac['inspections'][iso] = insp
        fac['newest_hit'] = max(fac['newest_hit'], iso)
    if other_cities:
        log.info(f"San Antonio: {sum(other_cities.values())} results list another "
                 f"city token (recorded as San Antonio): "
                 f"{dict(sorted(other_cities.items(), key=lambda kv: -kv[1])[:5])}")

    if not fetch_violations:
        # Debug path only (--no-dfw-violations): search-only records with no
        # score or violations, like Houston's.
        results = []
        for fac in facilities.values():
            for iso, insp in fac['inspections'].items():
                insp_date = sane_inspection_date(iso)
                if not insp_date or insp_date > until_iso:
                    continue
                results.append({
                    'name': fac['name'].title() if fac['name'].isupper() else fac['name'],
                    'address': fac['street'].title() if fac['street'].isupper() else fac['street'],
                    'city': 'San Antonio', 'state': 'TX', 'zip': fac['zip'],
                    'latitude': None, 'longitude': None,
                    'inspection_date': insp_date, 'original_score': None,
                    'risk_score': 100, 'priority_violations': 0,
                    'priority_foundation_violations': 0, 'core_violations': 0,
                    'total_violations': 0, 'violations': [],
                    'inspection_type': '', 'results': '',
                    'source': 'San Antonio Metro Health',
                    'source_url': (f"{SA_BASE}estab.cfm?licenseID={fac['license_id']}"
                                   f"&inspectionID={insp}"),
                    'source_id': f"sa_{fac['license_id']}_{insp}", 'metro': '',
                })
        if complete:
            _note_source_lag('sanantonio', newest)
        log.info(f"San Antonio: {len(results)} inspections (search only)")
        return results

    if facilities and _past_deadline():
        # Same rule as Houston: nothing has a score or violations yet, so
        # nothing may ship — the merge keeps existing data and a later run
        # refetches the window.
        log.warning(f"San Antonio: fetch deadline reached before the detail phase — "
                    f"dropping {len(facilities)} undetailed facilities")
        return []

    results = []
    if facilities:
        # Oldest search hit first, so a deadline stop ships the older days
        # complete and the lookback refetches the newer ones.
        order = sorted(facilities.values(), key=lambda f: f['newest_hit'])
        log.info(f"San Antonio: fetching estab pages for {len(order)} facilities "
                 f"({TYLER_DETAIL_WORKERS} workers)...")
        local = threading.local()

        def get_session():
            if not hasattr(local, 's'):
                local.s = _sa_session()
            return local.s

        def _sa_hold(fac, counted=True):
            days = [iso for iso in fac['inspections'] if since_iso <= iso <= until_iso]
            _hold_freshness('San Antonio Metro Health', min(days) if days else fac['newest_hit'],
                            counted, key=fac['license_id'])

        def work(fac):
            if _past_deadline():
                return fac, 'deadline'
            blocks = _sa_fetch_facility(get_session(), fac['license_id'])
            time.sleep(TYLER_DETAIL_DELAY)
            return fac, blocks

        counts = {'done': 0, 'ok': 0, 'page_only': 0, 'noscore': 0,
                  'missing': 0, 'deadline': 0}

        def apply_blocks(fac, blocks):
            recs = _sa_build_records(fac, blocks, since_iso, until_iso)
            counts['noscore'] += sum(1 for r in recs if r['original_score'] is None)
            counts['page_only'] += sum(1 for r in recs
                                       if r['inspection_date'] not in fac['inspections'])
            shipped = {r['inspection_date'] for r in recs}
            counts['missing'] += sum(1 for iso in fac['inspections']
                                     if since_iso <= iso <= until_iso and iso not in shipped)
            results.extend(recs)
            counts['ok'] += 1

        consecutive_failures = 0
        failed = []
        with ThreadPoolExecutor(max_workers=TYLER_DETAIL_WORKERS) as pool:
            futures = [pool.submit(work, f) for f in order]
            for future in as_completed(futures):
                fac, blocks = future.result()
                counts['done'] += 1
                if blocks is None:
                    failed.append(fac)
                    consecutive_failures += 1
                    if consecutive_failures >= TYLER_ABORT_AFTER:
                        for f in futures:
                            f.cancel()
                        raise RuntimeError(
                            f"San Antonio: {consecutive_failures} consecutive detail "
                            f"failures — aborting instead of recording "
                            f"false-clean inspections")
                elif blocks == 'deadline':
                    counts['deadline'] += 1
                    _sa_hold(fac, counted=False)
                else:
                    consecutive_failures = 0
                    apply_blocks(fac, blocks)
                if counts['done'] % 200 == 0:
                    log.info(f"  San Antonio details: {counts['done']}/{len(order)} "
                             f"({len(results)} inspections)")

        if failed and not _past_deadline():
            log.warning(f"San Antonio: retrying {len(failed)} failed estab fetches")
            retry_session = _sa_session()
            still_failed = []
            for fac in failed:
                blocks = _sa_fetch_facility(retry_session, fac['license_id'])
                time.sleep(TYLER_DETAIL_DELAY)
                if blocks is None:
                    still_failed.append(fac)
                else:
                    apply_blocks(fac, blocks)
            failed = still_failed
        if failed:
            # A facility whose page could not be read ships nothing — never
            # a clean inspection; the freshness hold makes the lookback
            # refetch it next run.
            log.warning(f"San Antonio: dropping {len(failed)} facilities with "
                        f"unreadable estab pages")
            retried = not _past_deadline()
            for fac in failed:
                _sa_hold(fac, counted=retried)
        if counts['deadline']:
            log.warning(f"San Antonio: fetch deadline reached — {counts['deadline']} "
                        f"facilities left undetailed (dropped)")
            _set_group_status('sanantonio', 'skipped',
                              f"fetch deadline reached; {counts['deadline']} facilities not read")
        log.info(f"San Antonio: {counts['ok']}/{len(order)} estab pages parsed; "
                 f"{len(results)} inspections in window ({counts['page_only']} found only "
                 f"on the facility page, {counts['noscore']} without a score, "
                 f"{counts['missing']} search hits missing from their facility page)")

    if limit:
        results = results[:limit]
    if complete:
        _note_source_lag('sanantonio', newest)
    log.info(f"San Antonio: {len(results)} inspections")
    return results


# ─── WASHINGTON DC (Tyler healthinspections.us portal) ──────────────────────

DC_BASE = 'https://dc.healthinspections.us/'
DC_DELAY = 1.5


def _dc_parse_report(html):
    """
    Parse a DC inspection report page: official Priority / Priority
    Foundation / Core counts plus the OBSERVATIONS item texts.
    """
    counts = {}
    for label, key in (('Priority', 'priority'),
                       ('Priority Foundation', 'priority_foundation'),
                       ('Core', 'core')):
        m = re.search(re.escape(label) + r'\s*</[^>]+>[^<]*<[^>]*>\s*Violations\s*'
                      r'</[^>]+>[^<]*<[^>]*>\s*(?:&nbsp;)*\s*(\d+)', html)
        if not m:
            m = re.search(re.escape(label) + r'\s*(?:<[^>]+>|\s|&nbsp;)*Violations'
                          r'(?:<[^>]+>|\s|&nbsp;)*(\d+)', html)
        if m:
            counts[key] = int(m.group(1))

    observations = []
    i = html.find('OBSERVATIONS')
    if i >= 0:
        seg = html[i:i + 40000]
        text = re.sub(r'<[^>]+>', '\n', seg)
        text = html_lib.unescape(text)
        for m in re.finditer(r'^\s*(\d{1,2})\.\s*-\s*(.{10,600}?)$', text, re.M):
            desc = re.sub(r'\s+', ' ', m.group(2)).strip()
            observations.append(desc[:500])
    return counts, observations


def fetch_dc(since_date=None, until_date=None, limit=None, fetch_violations=True):
    """
    Fetch Washington DC food inspections from DC Health's Tyler portal
    (dc.healthinspections.us). Monthly window searches return all results in
    one response; each inspection links a full report page that publishes
    OFFICIAL Priority / Priority Foundation / Core violation counts plus
    observation text — the counts drive the risk score directly.
    """
    log.info(f"Fetching DC data (since={since_date}, limit={limit})")
    session = requests.Session()
    session.headers.update({'User-Agent': CHROME_UA})
    session.get(DC_BASE + '?a=Inspections', timeout=30)

    start = datetime.strptime(str(since_date)[:10], '%Y-%m-%d') if since_date \
        else datetime(datetime.now().year, 1, 1)
    end = datetime.now()
    if until_date:
        end = min(end, datetime.strptime(str(until_date)[:10], '%Y-%m-%d'))

    months = []
    cur = datetime(start.year, start.month, 1)
    while cur <= end:
        nxt = datetime(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)
        months.append((max(cur, start), min(nxt - timedelta(days=1), end)))
        cur = nxt

    est_re = re.compile(
        r'<h3><a href="\?a=inspections&permitID=(\d+)">([^<]+)</a></h3>\s*'
        r'([^<]+?)<br\s*/>\s*Ward:\s*([^|<]*)\|[^<]*<br\s*/>\s*Type:\s*([^<\n]*)'
        r'(.*?)</div>\s*</li>', re.S)
    insp_re = re.compile(
        r'href="\.\./lib/mod/inspection/paper/(_paper_food_inspection_report\.cfm\?inspectionID=(\d+)[^"]*)"[^>]*>'
        r'\s*([^:<]+):\s*\w+,\s*(\w+ \d{1,2}, \d{4})')

    raw = []
    seen = set()
    for ws, we in months:
        if _past_deadline():
            log.warning(f"  DC: fetch deadline reached — stopping search "
                        f"with {len(raw)} inspections")
            break
        data = {
            'a': 'Inspections', 'inputEstabName': '', 'inputPermitType': '',
            'inputInspType': '', 'inputWard': '', 'inputQuad': '',
            'startDate': ws.strftime('%m/%d/%Y'), 'endDate': we.strftime('%m/%d/%Y'),
            'btnSearch': 'Search',
        }
        r = _tyler_request(session, 'POST', DC_BASE + 'index.cfm', data=data,
                           headers={'Referer': DC_BASE + '?a=Inspections'})
        if r is None:
            raise RuntimeError('DC search failed after retries')
        month_count = 0
        for em in est_re.finditer(r.text):
            permit_id, name, addr_line, ward, ftype, insp_block = em.groups()
            # Format: "716 MONROE ST NE WASHINGTON, DC 20017" — the city sits
            # between a GREEDY street match and the ", ST zip" tail. Known
            # two-word suburb names are checked before the one-word default.
            addr = addr_line.strip()
            am = re.match(
                r'^(.*?)\s+((?:FALLS CHURCH|SILVER SPRING|OXON HILL|COLLEGE PARK|'
                r'TAKOMA PARK|CAPITOL HEIGHTS|[A-Za-z.]+))\s*,?\s+([A-Z]{2})\s+(\d{5})',
                addr, re.I)
            if am and am.group(2):
                # re-run greedily so the street keeps everything but the city
                street_end = addr.rfind(am.group(2))
                street = addr[:street_end].strip()
                city, state, zip_code = am.group(2), am.group(3), am.group(4)
                if not street:
                    street, city = city, 'Washington'
            else:
                street, city, state, zip_code = addr, 'Washington', 'DC', ''
            for im in insp_re.finditer(insp_block):
                report_path, insp_id, insp_type, date_words = im.groups()
                if insp_id in seen:
                    continue
                seen.add(insp_id)
                try:
                    d = datetime.strptime(date_words, '%B %d, %Y').strftime('%Y-%m-%d')
                except ValueError:
                    continue
                raw.append({
                    'permit_id': permit_id, 'insp_id': insp_id,
                    'report_path': report_path,
                    'name': name.strip(), 'street': street, 'city': city.strip().title(),
                    'state': state, 'zip': zip_code, 'type': insp_type.strip(),
                    'date': d,
                })
                month_count += 1
        log.info(f"  DC {ws:%Y-%m}: {month_count} inspections (total {len(raw)})")
        if limit and len(raw) >= limit:
            raw = raw[:limit]
            break
        time.sleep(DC_DELAY)

    results = []
    for item in raw:
        insp_date = sane_inspection_date(item['date'])
        if not insp_date or not item['name']:
            continue
        # Keep DC locations only: mobile-vendor records carry their VA/MD
        # commissary addresses (not dining locations), and suburb names like
        # "Arlington, VA" would collide with same-named cities in other states.
        if (item['state'] or 'DC') != 'DC':
            continue
        results.append({
            'name': item['name'].title() if item['name'].isupper() else item['name'],
            'address': item['street'].title() if item['street'].isupper() else item['street'],
            'city': item['city'] or 'Washington',
            'state': item['state'] or 'DC',
            'zip': item['zip'],
            'latitude': None,
            'longitude': None,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': 100,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': item['type'],
            'results': '',
            'source': 'DC Health',
            'source_url': f"{DC_BASE}?a=inspections&permitID={item['permit_id']}",
            'source_id': f"dc_{item['insp_id']}",
            'metro': '',
            '_dc_report': item['report_path'],
        })

    if fetch_violations and results and _past_deadline():
        # Same rule as Houston: search-only records look clean and must not
        # ship undetailed — drop the run, keep existing data via the merge.
        log.warning(f"DC: fetch deadline reached before the report phase — "
                    f"dropping {len(results)} unparsed inspections")
        return []
    if fetch_violations and results:
        log.info(f"DC: fetching {len(results)} inspection reports "
                 f"({TYLER_DETAIL_WORKERS} workers)...")
        local = threading.local()

        def get_session():
            if not hasattr(local, 's'):
                local.s = requests.Session()
                local.s.headers.update({'User-Agent': CHROME_UA})
                local.s.get(DC_BASE + '?a=Inspections', timeout=30)
            return local.s

        def work(rec):
            url = DC_BASE + 'lib/mod/inspection/paper/' + rec['_dc_report']
            r = _tyler_request(get_session(), 'GET', url)
            time.sleep(TYLER_DETAIL_DELAY)
            if r is not None and r.status_code == 200:
                return rec, _dc_parse_report(r.text)
            # None = fetch failed; must not be recorded as a clean inspection
            return rec, None

        def apply_report(rec, counts, observations):
            pv = counts.get('priority', 0)
            pfv = counts.get('priority_foundation', 0)
            cv = counts.get('core', 0)
            violations = [list(classify_violation(t)) for t in observations]
            if counts:
                # DC publishes official severity counts — use them for
                # the score rather than our regex-derived tallies.
                risk = max(0, 100 - pv * 5 - pfv * 2 - cv * 1)
            else:
                risk, pv, pfv, cv = calc_risk_score(
                    [tuple(v) for v in violations])
            rec.update(risk_score=risk, priority_violations=pv,
                       priority_foundation_violations=pfv, core_violations=cv,
                       total_violations=max(pv + pfv + cv, len(violations)),
                       violations=[[c, s, str(d)[:500]] for c, s, d in violations])

        done = withv = consecutive_failures = 0
        failed = []
        with ThreadPoolExecutor(max_workers=TYLER_DETAIL_WORKERS) as pool:
            futures = [pool.submit(work, r) for r in results]
            for future in as_completed(futures):
                rec, payload = future.result()
                done += 1
                if payload is None:
                    failed.append(rec)
                    consecutive_failures += 1
                    if consecutive_failures >= TYLER_ABORT_AFTER:
                        for f in futures:
                            f.cancel()
                        raise RuntimeError(
                            f"DC: {consecutive_failures} consecutive report "
                            f"failures — aborting instead of recording "
                            f"false-clean inspections")
                else:
                    consecutive_failures = 0
                    counts, observations = payload
                    if counts or observations:
                        apply_report(rec, counts, observations)
                        withv += 1
                if done % 200 == 0:
                    log.info(f"  DC reports: {done}/{len(results)} ({withv} parsed)")

        if failed:
            log.warning(f"DC: retrying {len(failed)} failed report fetches")
            retry_session = requests.Session()
            retry_session.headers.update({'User-Agent': CHROME_UA})
            try:
                retry_session.get(DC_BASE + '?a=Inspections', timeout=30)
            except requests.RequestException:
                pass
            still_failed = []
            for rec in failed:
                url = DC_BASE + 'lib/mod/inspection/paper/' + rec['_dc_report']
                r = _tyler_request(retry_session, 'GET', url)
                time.sleep(TYLER_DETAIL_DELAY)
                if r is not None and r.status_code == 200:
                    counts, observations = _dc_parse_report(r.text)
                    if counts or observations:
                        apply_report(rec, counts, observations)
                        withv += 1
                else:
                    still_failed.append(rec)
            if still_failed:
                log.warning(f"DC: dropping {len(still_failed)} inspections "
                            f"with unreadable reports")
                drop = {id(r) for r in still_failed}
                results = [r for r in results if id(r) not in drop]
        log.info(f"DC: {withv}/{len(results)} reports parsed")

    for rec in results:
        rec.pop('_dc_report', None)
    log.info(f"DC: {len(results)} inspections")
    return results


# ─── FLORIDA DBPR (statewide) ────────────────────────────────────────────────

FL_EXTRACTS = 'https://www2.myfloridalicense.com/sto/file_download/extracts/'
# All seven DBPR district extract files = complete statewide coverage:
# 1: Miami-Dade/Keys, 2: Broward/Palm Beach, 3: Tampa Bay/Polk,
# 4: Orlando/Space Coast/Volusia, 5: Jacksonville/Gainesville,
# 6: Panhandle (Pensacola/Tallahassee), 7: Southwest (Ft Myers/Sarasota/Naples)
FL_DISTRICT_FILES = [f'{i}fdinspi.csv' for i in range(1, 8)]


def fetch_florida(since_date=None, limit=None):
    """
    Fetch Florida DBPR statewide food inspections (current-fiscal-year CSV
    extracts for all seven districts, updated continuously).
    Florida publishes OFFICIAL High Priority / Intermediate / Basic violation
    counts per inspection, which map directly to priority / priority
    foundation / core. No violation text or coordinates in the extracts —
    records are Census-geocoded downstream.

    NOTE: the extracts roll over each July 1 with the state fiscal year, so
    history accumulates from FY start; the weekly refresh keeps it growing.
    """
    log.info(f"Fetching Florida DBPR data (since={since_date}, limit={limit})")
    import csv as csv_mod
    import io

    results = []
    for fname in FL_DISTRICT_FILES:
        try:
            r = requests.get(FL_EXTRACTS + fname,
                             headers={'User-Agent': CHROME_UA}, timeout=180)
            r.raise_for_status()
        except requests.RequestException as e:
            log.error(f"  Florida {fname} download failed: {e}")
            continue
        reader = csv_mod.reader(io.StringIO(r.content.decode('latin-1')))
        header = next(reader, None)
        if not header:
            continue
        idx = {c.strip(): i for i, c in enumerate(header)}
        kept = 0
        for row in reader:
            try:
                if row[idx['Inspection Class']].strip() != 'Food':
                    continue
                raw_date = row[idx['Inspection Date']].strip()
                insp_date = sane_inspection_date(
                    datetime.strptime(raw_date, '%m/%d/%Y').strftime('%Y-%m-%d'))
                if not insp_date:
                    continue
                if since_date and insp_date < str(since_date)[:10]:
                    continue
                name = row[idx['Business (DBA-Does Business As) Name']].strip()
                if not name:
                    continue
                hp = int(row[idx['Number of High Priority Violations']] or 0)
                inter = int(row[idx['Number of Intermediate Violations']] or 0)
                basic = int(row[idx['Number of Basic Violations']] or 0)
                total = int(row[idx['Number of Total Violations']] or 0)
                city = row[idx['Location City']].strip().title()
                results.append({
                    'name': name.title() if name.isupper() else name,
                    'address': row[idx['Location Address']].strip().title(),
                    'city': city,
                    'state': 'FL',
                    'zip': row[idx['Location Zip Code']].strip()[:5],
                    'latitude': None,
                    'longitude': None,
                    'inspection_date': insp_date,
                    'original_score': None,
                    'risk_score': max(0, 100 - hp * 5 - inter * 2 - basic * 1),
                    'priority_violations': hp,
                    'priority_foundation_violations': inter,
                    'core_violations': basic,
                    'total_violations': total or (hp + inter + basic),
                    'violations': [],
                    'inspection_type': row[idx['Inspection Type']].strip(),
                    'results': row[idx['Inspection Disposition']].strip(),
                    'source': 'Florida DBPR',
                    'source_url': 'https://www2.myfloridalicense.com/hotels-restaurants/public-records/inspection-records/',
                    'source_id': f"fl_{row[idx['Inspection Number']]}_{row[idx['Visit Number']]}",
                    'metro': '',
                })
                kept += 1
                if limit and kept >= limit:
                    break
            except (IndexError, KeyError, ValueError):
                continue
        log.info(f"  Florida {fname}: {kept} inspections kept")
        if limit and len(results) >= limit:
            break
    log.info(f"Florida: {len(results)} inspections")
    return results


# ─── NEW YORK STATE (NYSDOH, statewide excluding NYC) ───────────────────────

NYS_URL = 'https://health.data.ny.gov/resource/cnih-y5dw.json'
# NYC's five boroughs run their own DOHMH inspection program (fetched by
# fetch_nyc with richer data), so exclude them here to avoid double coverage.
NYS_EXCLUDE_COUNTIES = {'BRONX', 'KINGS', 'NEW YORK', 'QUEENS', 'RICHMOND'}
# Violations text is a run of items whose descriptions may themselves contain
# semicolons, so split on the "Item <code>-" boundaries. Critical items are
# prefixed "Critical Violation [RED]"; non-critical items have NO marker:
#   "Item  5E-  Critical Violation [RED] <desc>; Item 10B-   <desc>; ..."
NYS_ITEM_SPLIT_RE = re.compile(r'\bItem\s+([0-9]{1,2}[A-Za-z]?)-\s*')
NYS_CRIT_PREFIX_RE = re.compile(
    r'^(Not\s+)?Critical\s+Violation\s*(?:\[RED\])?\s*[-:]?\s*', re.I)


def fetch_nys(since_date=None, limit=None):
    """
    Fetch New York State food service inspections (NYSDOH cnih-y5dw): every
    active permitted facility statewide with its most recent inspection —
    Rochester, Syracuse, Albany, Yonkers, Westchester, Long Island, and the
    rest of the state outside NYC. Publishes OFFICIAL critical/non-critical
    violation counts, full violation text, and coordinates. Buffalo (Erie
    County) runs an independent system and is absent from the state feed.
    Weekly refreshes accumulate inspection history over time.
    """
    log.info(f"Fetching NY State data (since={since_date}, limit={limit})")
    params = {
        '$order': 'date DESC',
        '$limit': min(limit or 50000, 50000),
        '$offset': 0,
    }
    if since_date:
        params['$where'] = f"date > '{str(since_date)[:10]}'"

    rows = _socrata_fetch_pages(NYS_URL, params, limit)
    log.info(f"NY State: {len(rows)} facility rows")

    results = []
    for rec in rows:
        name = (rec.get('operation_name') or rec.get('facility') or '').strip()
        insp_date = sane_inspection_date(str(rec.get('date') or '')[:10])
        county = (rec.get('county') or '').strip().upper()
        if not name or not insp_date or county in NYS_EXCLUDE_COUNTIES:
            continue

        # Official severity counts; violation text for detail/categories.
        try:
            crit = int(rec.get('total_critical_violations') or 0)
            noncrit = int(rec.get('total_noncritical_violations') or 0)
        except (TypeError, ValueError):
            crit = noncrit = 0

        violations = []
        vtext = (rec.get('violations') or '').strip()
        if vtext and not vtext.lower().startswith('no violations'):
            parts = NYS_ITEM_SPLIT_RE.split(vtext)
            # parts = [pre, code1, desc1, code2, desc2, ...]
            for k in range(1, len(parts) - 1, 2):
                code, desc = parts[k], parts[k + 1]
                desc = desc.strip().rstrip(';').strip()
                m = NYS_CRIT_PREFIX_RE.match(desc)
                sev = 'core'
                if m:
                    sev = 'core' if m.group(1) else 'priority'
                    desc = desc[m.end():].strip()
                desc = re.sub(r'�(?=[CF])', '°', desc)  # mangled degree signs
                if not desc:
                    continue
                cat, _, _ = classify_violation(desc)
                violations.append((cat, sev, f'Item {code}: {desc}'[:500]))

        # Trust the official totals for scoring; fall back to parsed items
        # if the totals are absent.
        if crit == 0 and noncrit == 0 and violations:
            _, crit, _pf, noncrit = calc_risk_score(violations)

        lat = lng = None
        loc = rec.get('location1') or {}
        try:
            lat = float(loc.get('latitude'))
            lng = float(loc.get('longitude'))
            # The source has hand-entered coords with occasional typos:
            # dropped minus signs, extra digits, lat copied into lng.
            # Repair the dropped-minus case; null anything else outside
            # the state so the Census geocoder rebuilds it from the address.
            if lng > 0 and 40.4 < lat < 45.2 and 71.7 < lng < 80.0:
                lng = -lng
            if not (40.4 < lat < 45.2 and -80.0 < lng < -71.7):
                lat = lng = None
        except (TypeError, ValueError, AttributeError):
            lat = lng = None

        city = (rec.get('municipality') or rec.get('city') or '').strip()
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (rec.get('facility_address') or rec.get('address') or '').strip().title(),
            'city': city.title() if city.isupper() else city,
            'state': 'NY',
            'zip': (rec.get('zip_code') or '').strip()[:5],
            'latitude': lat,
            'longitude': lng,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': max(0, 100 - crit * 5 - noncrit * 1),
            'priority_violations': crit,
            'priority_foundation_violations': 0,
            'core_violations': noncrit,
            'total_violations': crit + noncrit,
            'violations': [[c, s, d] for c, s, d in violations],
            'inspection_type': (rec.get('inspection_type') or '').strip(),
            'results': '',
            'source': 'NYS Department of Health',
            'source_url': 'https://health.data.ny.gov/Health/Food-Service-Establishment-Last-Inspection/cnih-y5dw',
            'source_id': f"nys_{rec.get('nys_health_operation_id')}_{insp_date}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"NY State: {len(results)} inspections")
    return results


# ─── RALEIGH / WAKE COUNTY, NC (ArcGIS open data) ───────────────────────────

WAKE_BASE = ('https://maps.wake.gov/arcgis/rest/services/Inspections/'
             'RestaurantInspectionsOpenData/MapServer')


def _arcgis_query_all(layer_url, where='1=1', out_fields='*', page_size=2000,
                      strict=False, object_id_field='OBJECTID'):
    """Fetch every row of an ArcGIS layer via resultOffset pagination."""
    rows = []
    offset = 0
    last_oid = -1
    while True:
        for attempt in (1, 2, 3):
            r = requests.get(layer_url + '/query', params={
                'where': where, 'outFields': out_fields, 'outSR': '4326',
                'orderByFields': object_id_field, 'resultOffset': offset,
                'resultRecordCount': page_size, 'f': 'json',
            }, headers={'User-Agent': CHROME_UA}, timeout=120)
            # Hosted feature services intermittently 5xx on deep pagination;
            # back off and retry before giving up on the whole layer.
            if r.status_code < 500 or attempt == 3:
                break
            time.sleep(2 * attempt)
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            raise RuntimeError(f"ArcGIS error: {data['error'].get('message')}")
        feats = data.get('features', [])
        if strict:
            if not isinstance(data.get('features'), list) or (
                    not feats and data.get('exceededTransferLimit')):
                raise RuntimeError('ArcGIS inventory page missing or incomplete')
            for feature in feats:
                oid = feature.get('attributes', {}).get(object_id_field)
                if not isinstance(oid, int) or oid <= last_oid:
                    raise RuntimeError('ArcGIS inventory pagination did not advance')
                last_oid = oid
        rows.extend(feats)
        # Servers silently clamp responses to their maxRecordCount (which can
        # be smaller than page_size) — only stop when the server says the
        # transfer limit was NOT exceeded AND the page came back short.
        if not feats or (len(feats) < page_size
                         and not data.get('exceededTransferLimit')):
            return rows
        offset += len(feats)
        time.sleep(0.3)


def fetch_wake(since_date=None, limit=None):
    """
    Fetch Raleigh / Wake County, NC inspections from the county's ArcGIS
    open-data service: a restaurants layer (with coordinates), an inspections
    layer carrying the OFFICIAL North Carolina 0-100 sanitation score, and a
    violations layer with item text and point deductions. Updated daily.
    """
    log.info(f"Fetching Wake County data (since={since_date}, limit={limit})")
    sd = str(since_date)[:10] if since_date else '2024-01-01'

    rest_rows = _arcgis_query_all(
        f'{WAKE_BASE}/0',
        out_fields='HSISID,NAME,ADDRESS1,CITY,STATE,POSTALCODE,X,Y')
    facilities = {}
    for f in rest_rows:
        a = f.get('attributes', {})
        if a.get('HSISID'):
            facilities[str(a['HSISID'])] = a
    log.info(f"  Wake: {len(facilities)} facilities")

    insp_rows = _arcgis_query_all(
        f'{WAKE_BASE}/1', where=f"DATE_ >= DATE '{sd}'",
        out_fields='HSISID,SCORE,DATE_,TYPE')
    log.info(f"  Wake: {len(insp_rows)} inspections since {sd}")

    viol_rows = _arcgis_query_all(
        f'{WAKE_BASE}/2', where=f"INSPECTDATE >= DATE '{sd}'",
        out_fields='HSISID,INSPECTDATE,SHORTDESC,COMMENTS,POINTVALUE')
    by_key = defaultdict(list)
    for f in viol_rows:
        a = f.get('attributes', {})
        ts = a.get('INSPECTDATE')
        if not a.get('HSISID') or not ts:
            continue
        d = datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d')
        by_key[(str(a['HSISID']), d)].append(a)
    log.info(f"  Wake: {len(viol_rows)} violation rows")

    results = []
    for f in insp_rows:
        a = f.get('attributes', {})
        hsis = str(a.get('HSISID') or '')
        fac = facilities.get(hsis)
        ts = a.get('DATE_')
        if not fac or not ts:
            continue
        insp_date = sane_inspection_date(
            datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d'))
        if not insp_date:
            continue
        try:
            score = round(float(a.get('SCORE')))
        except (TypeError, ValueError):
            continue
        if score <= 0:  # unscored visit (e.g. status check) — no grade basis
            continue
        name = (fac.get('NAME') or '').strip()
        if not name:
            continue

        violations = []
        pv = pfv = cv = 0
        for v in by_key.get((hsis, insp_date), []):
            desc = ' '.join(x for x in [(v.get('SHORTDESC') or '').strip(),
                                        (v.get('COMMENTS') or '').strip()] if x)
            if not desc:
                continue
            try:
                pts = float(v.get('POINTVALUE') or 0)
            except (TypeError, ValueError):
                pts = 0.0
            # NC deducts up to 3 points per item; anchor severity to the
            # deduction actually assessed on this inspection.
            if pts >= 1.5:
                sev = 'priority'; pv += 1
            elif pts >= 0.5:
                sev = 'priority_foundation'; pfv += 1
            else:
                sev = 'core'; cv += 1
            cat, _, _ = classify_violation(desc)
            violations.append([cat, sev, desc[:500]])

        city = (fac.get('CITY') or 'Raleigh').strip().title()
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (fac.get('ADDRESS1') or '').strip().title(),
            'city': city,
            'state': 'NC',
            'zip': (str(fac.get('POSTALCODE') or ''))[:5],
            'latitude': fac.get('Y'),
            'longitude': fac.get('X'),
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),  # official NC 0-100 score
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': (a.get('TYPE') or '').strip(),
            'results': '',
            'source': 'Wake County Open Data',
            'source_url': 'https://data.wake.gov/datasets/Wake::food-inspections/about',
            'source_id': f'wake_{hsis}_{insp_date}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Wake County: {len(results)} inspections")
    return results


# ─── LOS ANGELES COUNTY (Environmental Health hosted CSVs) ───────────────────
# LA County DPH Environmental Health covers unincorporated areas plus 85 of
# the county's 88 cities (all except Long Beach, Pasadena, and Vernon). The
# open-data portal (data.lacounty.gov) publishes rolling three-year extracts
# as hosted CSV items on the county ArcGIS hub, released quarterly:
#   inspections — one row per inspection with the official 0-100 SCORE
#   violations  — join on SERIAL NUMBER; POINTS is the deduction assessed
#                 (4 = major/critical, 2 = minor critical, 1 = good practice)
#   inventory   — active facilities with coordinates, keyed by FACILITY ID
# The CSVs are Windows-1252 encoded and dates are MM/DD/YYYY.

LA_HUB_ITEM_URL = 'https://www.arcgis.com/sharing/rest/content/items/{item}/data'
LA_INSPECTIONS_ITEM = '19b6607ac82c4512b10811870975dbdc'
LA_VIOLATIONS_ITEM = '5eaea9f89b7549ee841da7617d3a9cba'
# The inventory moved to a Feature Service around 2026-10-13 and its old CSV
# item (4f31c9a99e444a40a3806e3bbe7b5fdd) was deleted, which failed the
# 2026-10-18 refresh. Discovery finds the current item; this id is only the
# fallback when the hub search itself fails, so keep it on a live item.
LA_INVENTORY_ITEM = '6715145614f34cce8ac7ded3d8b3a508'
LA_SOURCE_PAGE = ('https://data.lacounty.gov/datasets/'
                  'lacounty::environmental-health-restaurant-and-market-inspections')
LA_HUB_SEARCH_URL = 'https://www.arcgis.com/sharing/rest/search'
LA_HUB_OWNER = 'CMarquez@ph.lacounty.gov_lacounty'
LA_HUB_TITLE_RE = re.compile(
    r'^Environmental Health Restaurant and Market\s+(Inspections|Violations|Inventory)\b')


def _la_hub_items():
    """Resolve the current county-published CSVs and inventory Feature Service.

    Quarterly extracts can update in place or move to a new hub item and
    format. A hardcoded id can freeze or disappear. Search the hub for
    the item with the latest end date per file (ties broken by modified
    time) and fall back to the known ids if the search fails.
    """
    items = {'inspections': LA_INSPECTIONS_ITEM, 'violations': LA_VIOLATIONS_ITEM,
             'inventory': LA_INVENTORY_ITEM}
    try:
        candidates = []
        start = 1
        while start != -1:
            r = requests.get(LA_HUB_SEARCH_URL, params={
                'q': f'owner:{LA_HUB_OWNER} (type:CSV OR type:"Feature Service") '
                     f'title:"Environmental Health Restaurant and Market"',
                'f': 'json', 'num': 50, 'start': start}, timeout=60)
            r.raise_for_status()
            data = r.json()
            if 'error' in data or not isinstance(data.get('results'), list):
                raise RuntimeError('Invalid LA hub discovery response')
            candidates.extend(data['results'])
            next_start = data.get('nextStart', -1)
            if next_start != -1 and (not isinstance(next_start, int) or next_start <= start):
                raise RuntimeError('LA hub discovery pagination did not advance')
            start = next_start
        best = {}
        for it in candidates:
            title = it.get('title') or ''
            m = LA_HUB_TITLE_RE.match(title)
            if not m or it.get('owner') != LA_HUB_OWNER:
                continue
            kind = m.group(1).lower()
            if it.get('type') != 'CSV' and not (
                    kind == 'inventory' and it.get('type') == 'Feature Service'):
                continue
            dates = re.findall(r'(\d{2})/(\d{2})/(\d{4}|\d{2})(?!\d)', title)
            end_date = ''
            if dates:
                month, day, year = dates[-1]
                year = '20' + year if len(year) == 2 else year
                try:
                    end_date = datetime.strptime(
                        f'{month}/{day}/{year}', '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    continue
            key = (end_date, it.get('modified') or 0)
            if kind not in best or key > best[kind][0]:
                best[kind] = (key, it['id'], title)
        for kind, (_, item_id, title) in best.items():
            if item_id != items[kind]:
                log.info(f"  LA: newer hub item for {kind}: {item_id} ({title})")
            items[kind] = item_id
    except Exception as e:
        log.warning(f"  LA: hub item discovery failed ({e}) — using the known item ids")
    return items


def _la_csv_rows(item_id):
    """Download a hosted-CSV hub item; yield dicts with normalized keys.

    Header names drift between extracts ('FACILITY  STATE' has a double
    space in the inventory file), so keys are whitespace-collapsed.
    """
    import csv as _csv
    import io as _io
    resp = requests.get(LA_HUB_ITEM_URL.format(item=item_id), timeout=600)
    resp.raise_for_status()
    text = resp.content.decode('cp1252', errors='replace')
    for row in _csv.DictReader(_io.StringIO(text)):
        yield {re.sub(r'\s+', ' ', (k or '')).strip(): (v or '').strip()
               for k, v in row.items()}


def _la_inventory_rows(item_id):
    """Read CSV or a single-layer inventory service from the trusted publisher.

    Resolve the service URL from item metadata rather than freezing a dated
    service name in code. Refuse schema changes instead of silently losing
    every coordinate. API errors (including HTTP-200 ArcGIS errors) propagate
    to the fetch group's existing source-health reporting.
    """
    def metadata(url):
        response = requests.get(url, params={'f': 'json'}, timeout=60)
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            raise RuntimeError(f"LA inventory metadata error: {data['error']}")
        return data

    item = metadata(LA_HUB_ITEM_URL.format(item=item_id).removesuffix('/data'))
    if item.get('owner') != LA_HUB_OWNER:
        raise RuntimeError('LA inventory publisher mismatch')
    if item.get('type') == 'CSV':
        yield from _la_csv_rows(item_id)
        return
    if item.get('type') != 'Feature Service':
        raise RuntimeError('Unsupported LA inventory item type')
    service = (item.get('url') or '').rstrip('/')
    if not re.fullmatch(r'https://services\d*\.arcgis\.com/[A-Za-z0-9]+/'
                        r'arcgis/rest/services/[^/?#]+/FeatureServer', service):
        raise RuntimeError('Invalid LA inventory service URL')
    layers = metadata(service).get('layers', [])
    if len(layers) != 1 or not isinstance(layers[0].get('id'), int):
        raise RuntimeError('LA inventory must have one unambiguous layer')
    layer = f"{service}/{layers[0]['id']}"
    layer_info = metadata(layer)
    fields = {f['name'] for f in layer_info.get('fields', [])}
    oid_field = layer_info.get('objectIdField')
    if (not oid_field or oid_field not in fields
            or not {'FACILITY_ID', 'LATITUDE', 'LONGITUDE'} <= fields):
        raise RuntimeError('LA inventory coordinate schema changed')
    for feature in _arcgis_query_all(
            layer, out_fields=f'{oid_field},FACILITY_ID,LATITUDE,LONGITUDE',
            strict=True, object_id_field=oid_field):
        attributes = feature['attributes']
        yield {'FACILITY ID': attributes.get('FACILITY_ID'),
               'FACILITY LATITUDE': attributes.get('LATITUDE'),
               'FACILITY LONGITUDE': attributes.get('LONGITUDE')}


def _la_date(mdy):
    """'07/01/2023' → '2023-07-01' (None if unparseable)."""
    try:
        return datetime.strptime(mdy.strip(), '%m/%d/%Y').strftime('%Y-%m-%d')
    except (ValueError, AttributeError):
        return None


def fetch_la(since_date=None, limit=None):
    """
    Fetch LA County restaurant & market inspections from the county's
    quarterly inspection and violation CSVs. Coordinates come from the
    facility inventory (CSV or Feature Service);
    facilities absent from it (recently closed or re-permitted) fall back
    to the Census batch geocoder downstream.
    """
    log.info(f"Fetching LA County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    items = _la_hub_items()

    coords = {}
    for row in _la_inventory_rows(items['inventory']):
        fid = row.get('FACILITY ID')
        try:
            lat = float(row.get('FACILITY LATITUDE') or 0)
            lng = float(row.get('FACILITY LONGITUDE') or 0)
        except (TypeError, ValueError):
            continue
        if (fid and math.isfinite(lat) and math.isfinite(lng)
                and -90 <= lat <= 90 and -180 <= lng <= 180 and lat and lng):
            coords[fid] = (lat, lng)
    log.info(f"  LA: {len(coords)} facilities with coordinates in inventory")

    inspections = []
    source_max = ''
    for row in _la_csv_rows(items['inspections']):
        insp_date = sane_inspection_date(_la_date(row.get('ACTIVITY DATE', '')))
        if insp_date and insp_date > source_max:
            source_max = insp_date
        if not insp_date or (since and insp_date < since):
            continue
        # INACTIVE programs are closed businesses (or prior ownership of a
        # re-permitted location) — don't resurrect them on the map.
        if row.get('PROGRAM STATUS') == 'INACTIVE':
            continue
        name = row.get('FACILITY NAME') or ''
        serial = row.get('SERIAL NUMBER') or ''
        try:
            score = int(float(row.get('SCORE')))
        except (TypeError, ValueError):
            continue
        if not name or not serial or score <= 0:
            continue
        inspections.append((serial, insp_date, score, row))
        if limit and len(inspections) >= limit:
            break
    log.info(f"  LA: {len(inspections)} scored inspections since {since or 'start'}")
    if not limit or len(inspections) < limit:
        # The whole extract was read, so source_max is the county's newest
        # row. Between quarterly releases the lookback still re-reads the
        # extract's last days, so the run ships records and the report's
        # STALE flag needs this to say the county has not published.
        _note_source_lag('LA County', source_max)
    if not inspections:
        # The county releases quarterly extracts, so a window past the file's
        # last day
        # legitimately returns nothing. Say which it is: _la_hub_items already
        # picks up the successor automatically once it appears.
        if source_max:
            log.warning(f"  LA: the county's extract itself ends {source_max} — "
                        f"nothing published for the requested window")
            _GROUP_NOTES['LA County'] = (
                f"quarterly county extract ends {source_max}; "
                f"no newer inspections published for this window")
        return []

    wanted = {serial for serial, _, _, _ in inspections}
    by_serial = defaultdict(list)
    for row in _la_csv_rows(items['violations']):
        serial = row.get('SERIAL NUMBER')
        if serial in wanted:
            by_serial[serial].append(row)
    log.info(f"  LA: violation rows matched for {len(by_serial)} inspections")

    results = []
    for serial, insp_date, score, row in inspections:
        violations = []
        pv = pfv = cv = 0
        for v in by_serial.get(serial, []):
            desc = re.sub(r'^#\s*\d+\.\s*', '', v.get('VIOLATION DESCRIPTION') or '')
            if not desc:
                continue
            try:
                pts = float(v.get('POINTS') or 0)
            except ValueError:
                pts = 0.0
            if pts >= 4:
                sev = 'priority'; pv += 1
            elif pts >= 2:
                sev = 'priority_foundation'; pfv += 1
            else:
                sev = 'core'; cv += 1
            cat, _, _ = classify_violation(desc)
            violations.append([cat, sev, desc[:500]])

        name = row.get('FACILITY NAME') or ''
        address = row.get('FACILITY ADDRESS') or ''
        city = (row.get('FACILITY CITY') or 'Los Angeles').title()
        fid = row.get('FACILITY ID') or ''
        lat, lng = coords.get(fid, (None, None))
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': address.title() if address.isupper() else address,
            'city': city,
            'state': 'CA',
            'zip': (row.get('FACILITY ZIP') or '').split('-')[0][:5],
            'latitude': lat,
            'longitude': lng,
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),  # official LA County 0-100 score
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': row.get('SERVICE DESCRIPTION', ''),
            'results': row.get('GRADE', ''),
            'source': 'LA County Open Data',
            'source_url': LA_SOURCE_PAGE,
            'source_id': f'la_{serial}',
            'metro': 'Los Angeles',
        })
    log.info(f"LA County: {len(results)} inspections")
    return results


# ─── LOUISVILLE / JEFFERSON COUNTY, KY (Metro ArcGIS open data) ──────────────
# Louisville Metro Public Health & Wellness publishes inspections with the
# official Kentucky 0-100 score and letter grade. FoodServiceData keeps the
# ~3 most recent inspections per establishment (name/address/score/grade).
# The companion bulk violations table is a frozen extract (its newest
# inspection dates to 2022-02) so, like Austin, records carry the official
# score with no violation text. No layer carries coordinates — records
# geocode downstream (Census batch).

LOUISVILLE_BASE = ('https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/'
                   'services')
LOUISVILLE_SOURCE_PAGE = ('https://data.louisvilleky.gov/datasets/'
                          'louisville::food-service-data')


def fetch_louisville(since_date=None, limit=None):
    log.info(f"Fetching Louisville data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None

    where = "score IS NOT NULL"
    if since:
        # InspectionDate is an ISO YYYY-MM-DD string in this layer
        where += f" AND InspectionDate >= '{since}'"
    insp_rows = _arcgis_query_all(
        f'{LOUISVILLE_BASE}/FoodServiceData/FeatureServer/0',
        where=where,
        out_fields='EstablishmentID,InspectionID,Ins_TypeDesc,EstablishmentName,'
                   'Address,City,State,Zip,InspectionDate,score,Grade',
        page_size=1000)
    log.info(f"  Louisville: {len(insp_rows)} inspection rows")
    if not insp_rows:
        return []

    results = []
    for f in insp_rows:
        a = f.get('attributes', {})
        insp_date = sane_inspection_date(str(a.get('InspectionDate') or '')[:10])
        name = (a.get('EstablishmentName') or '').strip()
        try:
            score = int(a.get('score'))
        except (TypeError, ValueError):
            continue
        if not insp_date or not name or score <= 0:
            continue

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (a.get('Address') or '').strip().title(),
            'city': ((a.get('City') or 'Louisville').strip().title()) or 'Louisville',
            'state': 'KY',
            'zip': str(a.get('Zip') or '')[:5],
            'latitude': None,
            'longitude': None,
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),  # official KY 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': (a.get('Ins_TypeDesc') or '').strip(),
            'results': (a.get('Grade') or '').strip(),
            'source': 'Louisville Metro Open Data',
            'source_url': LOUISVILLE_SOURCE_PAGE,
            'source_id': f"lou_{a.get('InspectionID')}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Louisville: {len(results)} inspections")
    return results


# ─── MINNEAPOLIS, MN (city ArcGIS open data) ─────────────────────────────────
# Single layer, one row per violation (violation fields null on clean
# inspections), with the city's 0-100 InspectionScore and Minnesota
# Priority1/Priority2/Priority3 violation tiers. Coordinates included.

MPLS_LAYER = ('https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/'
              'services/Food_Inspections/FeatureServer/0')
MPLS_SOURCE_PAGE = ('https://opendata.minneapolismn.gov/datasets/'
                    'cityoflakes::food-inspections')
MPLS_SEVERITY = {'Priority1': 'priority', 'Priority2': 'priority_foundation',
                 'Priority3': 'core'}


def fetch_minneapolis(since_date=None, limit=None):
    log.info(f"Fetching Minneapolis data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    where = f"DateOfInspection >= DATE '{since}'" if since else '1=1'

    rows = _arcgis_query_all(
        MPLS_LAYER, where=where,
        out_fields='InspectionIDNumber,DateOfInspection,InspectionScore,'
                   'ViolationPriority,FoodCodeText,InspectorComments,'
                   'BusinessName,FullAddress,City,ZipCode,Latitude,Longitude,'
                   'InspectionType,InspectionResult')
    log.info(f"  Minneapolis: {len(rows)} violation-level rows")

    by_insp = defaultdict(list)
    for f in rows:
        a = f.get('attributes', {})
        if a.get('InspectionIDNumber'):
            by_insp[a['InspectionIDNumber']].append(a)

    results = []
    for insp_id, group in by_insp.items():
        a = group[0]
        ts = a.get('DateOfInspection')
        if not ts:
            continue
        insp_date = sane_inspection_date(
            datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d'))
        name = (a.get('BusinessName') or '').strip()
        try:
            score = int(a.get('InspectionScore'))
        except (TypeError, ValueError):
            continue
        if not insp_date or not name or score <= 0:
            continue

        violations = []
        pv = pfv = cv = 0
        for v in group:
            desc = ' '.join(x for x in [(v.get('FoodCodeText') or '').strip(),
                                        (v.get('InspectorComments') or '').strip()] if x)
            if not desc:
                continue  # clean inspections carry one row with null violation fields
            sev = MPLS_SEVERITY.get((v.get('ViolationPriority') or '').strip(), 'core')
            if sev == 'priority':
                pv += 1
            elif sev == 'priority_foundation':
                pfv += 1
            else:
                cv += 1
            cat, _, _ = classify_violation(desc)
            violations.append([cat, sev, desc[:500]])

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (a.get('FullAddress') or '').strip().title(),
            'city': ((a.get('City') or 'Minneapolis').strip().title()) or 'Minneapolis',
            'state': 'MN',
            'zip': str(a.get('ZipCode') or '')[:5],
            'latitude': a.get('Latitude'),
            'longitude': a.get('Longitude'),
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),  # official 0-100 score
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': (a.get('InspectionType') or '').strip(),
            'results': (a.get('InspectionResult') or '').strip(),
            'source': 'Minneapolis Open Data',
            'source_url': MPLS_SOURCE_PAGE,
            'source_id': f'mpls_{insp_id}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Minneapolis: {len(results)} inspections")
    return results


# ─── CINCINNATI, OH (Socrata) ────────────────────────────────────────────────
# One row per violation per inspection (clean inspections appear as rows with
# null violation fields), with coordinates and rich violation text but no
# numeric score — risk_score is derived from violation severities the same
# way as other scoreless sources (calc_risk_score).

CINCINNATI_URL = 'https://data.cincinnati-oh.gov/resource/rg6p-b3h3.json'


def _strip_wrapping_quotes(s):
    return (s or '').strip().strip('"').strip()


def fetch_cincinnati(since_date=None, limit=None):
    log.info(f"Fetching Cincinnati data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'
    params = {
        '$where': f"action_date >= '{since}'",
        '$order': 'action_date DESC',
        '$limit': 50000,
        '$offset': 0,
    }
    rows = _socrata_fetch_pages(CINCINNATI_URL, params, limit)
    log.info(f"  Cincinnati: {len(rows)} violation-level rows")

    by_insp = defaultdict(list)
    for rec in rows:
        key = rec.get('recordnum_insp')
        if key:
            by_insp[key].append(rec)

    results = []
    for insp_id, group in by_insp.items():
        rec = group[0]
        insp_date = sane_inspection_date(str(rec.get('action_date', ''))[:10])
        name = _strip_wrapping_quotes(rec.get('business_name'))
        if not insp_date or not name:
            continue

        violations = []
        for v in group:
            desc = _strip_wrapping_quotes(v.get('violation_description'))
            comments = _strip_wrapping_quotes(v.get('violation_comments'))
            # Drop the leading Ohio code citation ("3717-1-04.8(F) - Violation - ")
            desc = re.sub(r'^[\d.()\-A-Za-z]+\s*-\s*Violation\s*-\s*', '', desc)
            desc = ' '.join(x for x in [desc.replace('\n', ' ').strip(), comments] if x)
            if not desc:
                continue  # clean inspection row
            violations.append(classify_violation(desc[:500]))
        risk_score, pv, pfv, cv = calc_risk_score(violations)

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': _strip_wrapping_quotes(rec.get('address')).title(),
            'city': (_strip_wrapping_quotes(rec.get('city')) or 'Cincinnati').title(),
            'state': 'OH',
            'zip': (rec.get('postal_code') or '')[:5],
            'latitude': float(rec['latitude']) if rec.get('latitude') else None,
            'longitude': float(rec['longitude']) if rec.get('longitude') else None,
            'inspection_date': insp_date,
            'original_score': None,  # Cincinnati publishes no numeric score
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [list(v) for v in violations],
            'inspection_type': (rec.get('insp_type') or '').strip(),
            'results': (rec.get('action_status') or '').strip(),
            'source': 'Cincinnati Open Data',
            'source_url': CINCINNATI_URL,
            'source_id': f'cin_{insp_id}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Cincinnati: {len(results)} inspections")
    return results


# ─── DELAWARE (statewide, Socrata) ───────────────────────────────────────────
# Violations-only dataset (rolling ~2 years): an inspection appears only if it
# cited at least one violation, so absence is NOT evidence of a clean record.
# risk_score derives from violation severities; coordinates included.

DELAWARE_URL = 'https://data.delaware.gov/resource/384s-wygj.json'


def fetch_delaware(since_date=None, limit=None):
    log.info(f"Fetching Delaware data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'
    params = {
        '$where': f"insp_date >= '{since}'",
        '$order': 'insp_date DESC',
        '$limit': 50000,
        '$offset': 0,
    }
    rows = _socrata_fetch_pages(DELAWARE_URL, params, limit)
    log.info(f"  Delaware: {len(rows)} violation-level rows")

    by_insp = defaultdict(list)
    for rec in rows:
        name = (rec.get('restname') or '').strip()
        date = str(rec.get('insp_date', ''))[:10]
        if name and date:
            by_insp[(name, (rec.get('restaddress') or '').strip(), date)].append(rec)

    results = []
    for (name, address, date), group in by_insp.items():
        insp_date = sane_inspection_date(date)
        if not insp_date:
            continue
        rec = group[0]
        violations = []
        for v in group:
            desc = ' '.join(x for x in [(v.get('violation') or '').strip(),
                                        (v.get('vio_desc') or '').strip()] if x)
            if desc:
                violations.append(classify_violation(desc[:500]))
        risk_score, pv, pfv, cv = calc_risk_score(violations)

        geo = rec.get('geocoded_column') or {}
        lat = lng = None
        try:
            lat = float(geo.get('latitude'))
            lng = float(geo.get('longitude'))
        except (TypeError, ValueError):
            pass

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': address.title() if address.isupper() else address,
            'city': ((rec.get('restcity') or '').strip().title()) or 'Delaware',
            'state': 'DE',
            'zip': (rec.get('restzip') or '')[:5],
            'latitude': lat,
            'longitude': lng,
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [list(v) for v in violations],
            'inspection_type': (rec.get('insp_type') or '').strip(),
            'results': '',
            'source': 'Delaware Open Data',
            'source_url': DELAWARE_URL,
            'source_id': f'de_{make_restaurant_id(name, address, "")}_{insp_date}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Delaware: {len(results)} inspections")
    return results


# ─── MONTGOMERY COUNTY, MD (DC suburbs, Socrata) ─────────────────────────────
# Pass/Fail inspections with ~16 per-category compliance columns and no
# violation free-text. Non-compliant categories become pseudo-violations so
# risk derives from calc_risk_score; a Fail is capped below the F threshold.
# The raw table duplicates each inspection ~180x (snapshot appends) — the
# $group query dedupes server-side, then rows are merged per inspection.

MOCO_URL = 'https://data.montgomerycountymd.gov/resource/dkrp-gr48.json'
MOCO_CATEGORIES = {
    'food_from_approved_source': 'Food not from an approved source',
    'food_protected_from_contamination': 'Food not protected from contamination',
    'workers_restricted': 'Ill food workers not restricted from working',
    'proper_hand_washing': 'Improper hand washing',
    'cooling_time_and_temperature': 'Improper cooling time and temperature',
    'cold_holding_temperature': 'Improper cold holding temperature',
    'hot_holding_temperature': 'Improper hot holding temperature',
    'cooking_time_and_temperature': 'Improper cooking time and temperature',
    'reheating_time_and_temperature': 'Improper reheating time and temperature',
    'hot_and_cold_running_water_provided': 'Hot and cold running water not provided',
    'proper_sewage_disposal': 'Improper sewage disposal',
    'toxic_substances_and_pesticides': 'Toxic substances or pesticides not properly stored',
    'rodents_and_insects': 'Evidence of rodents or insects observed',
    'nutritional_labeling': 'Nutritional labeling not in compliance',
    'trans_fat_ban': 'Trans fat ban not in compliance',
    'nosmoking_sign_posted': 'No-smoking sign not posted',
}


def fetch_montgomery(since_date=None, limit=None):
    log.info(f"Fetching Montgomery County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None

    cols = ['inspection_number', 'business_name', 'address', 'city', 'state',
            'zip', 'inspection_type', 'inspection_start_date', 'status',
            'location'] + list(MOCO_CATEGORIES)
    params = {
        '$select': ','.join(cols),
        '$group': ','.join(cols),
        '$limit': 50000,
        '$offset': 0,
    }
    if since:
        params['$where'] = f"inspection_start_date >= '{since}'"
    rows = _socrata_fetch_pages(MOCO_URL, params, limit)
    log.info(f"  Montgomery: {len(rows)} deduped rows")

    # Snapshot variants of the same inspection can still differ in a column —
    # merge them: any variant out of compliance marks the category, any Fail
    # marks the inspection failed.
    by_insp = defaultdict(list)
    for rec in rows:
        key = rec.get('inspection_number')
        if key and rec.get('business_name'):
            by_insp[key].append(rec)

    results = []
    for insp_id, group in by_insp.items():
        rec = group[0]
        insp_date = sane_inspection_date(str(rec.get('inspection_start_date', ''))[:10])
        name = (rec.get('business_name') or '').strip()
        if not insp_date or not name:
            continue

        failed = any((v.get('status') or '').strip().lower() == 'fail' for v in group)
        out = set()
        for v in group:
            for col in MOCO_CATEGORIES:
                val = (v.get(col) or '').strip().lower()
                if val and 'not in compliance' in val:
                    out.add(col)
        violations = [classify_violation(MOCO_CATEGORIES[col]) for col in sorted(out)]
        risk_score, pv, pfv, cv = calc_risk_score(violations)
        if failed:
            risk_score = min(risk_score, 65)  # a failed inspection is an F

        geo = (rec.get('location') or {}).get('coordinates') or [None, None]
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (rec.get('address') or '').strip().title(),
            'city': ((rec.get('city') or '').strip().title()) or 'Montgomery County',
            'state': 'MD',
            'zip': (rec.get('zip') or '')[:5],
            'latitude': geo[1],
            'longitude': geo[0],
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [list(v) for v in violations],
            'inspection_type': (rec.get('inspection_type') or '').strip(),
            'results': (rec.get('status') or '').strip(),
            'source': 'Montgomery County Open Data',
            'source_url': MOCO_URL,
            'source_id': f'moco_{insp_id}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Montgomery County: {len(results)} inspections")
    return results


# ─── DETROIT, MI (city ArcGIS open data, 3-layer relational service) ─────────
# Published May 2025 (replaces a deprecated service that lacked addresses):
# layer 0 = establishments (points), layer 1 = inspections, layer 2 =
# violations with Michigan Priority/Foundation/Core classes. No numeric
# score — risk derives from violation severities (calc_risk_score).

DETROIT_BASE = ('https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/'
                'services/food_service_establishment_inspections/FeatureServer')
DETROIT_SEVERITY = {'Priority': 'priority', 'Foundation': 'priority_foundation',
                    'Core': 'core'}


def fetch_detroit(since_date=None, limit=None):
    log.info(f"Fetching Detroit data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'

    fac_rows = _arcgis_query_all(
        f'{DETROIT_BASE}/0',
        out_fields='establishment_id,establishment_name,address,city,zip_code,'
                   'latitude,longitude,establishment_status')
    facilities = {}
    for f in fac_rows:
        a = f.get('attributes', {})
        if a.get('establishment_id'):
            facilities[str(a['establishment_id'])] = a
    log.info(f"  Detroit: {len(facilities)} establishments")

    # inspection_date is an esriFieldTypeDateOnly string ("2026-06-23")
    insp_rows = _arcgis_query_all(
        f'{DETROIT_BASE}/1', where=f"inspection_date >= DATE '{since}'",
        out_fields='inspection_id,establishment_id,inspection_date,'
                   'inspection_type,is_in_compliance', page_size=1000)
    log.info(f"  Detroit: {len(insp_rows)} inspections since {since}")

    viol_rows = _arcgis_query_all(
        f'{DETROIT_BASE}/2', where=f"inspection_date >= DATE '{since}'",
        out_fields='inspection_id,violation_description,violation_type,'
                   'problem_description', page_size=1000)
    by_insp = defaultdict(list)
    for f in viol_rows:
        a = f.get('attributes', {})
        if a.get('inspection_id'):
            by_insp[str(a['inspection_id'])].append(a)
    log.info(f"  Detroit: {len(viol_rows)} violation rows")

    results = []
    for f in insp_rows:
        a = f.get('attributes', {})
        fac = facilities.get(str(a.get('establishment_id') or ''))
        insp_date = sane_inspection_date(str(a.get('inspection_date') or '')[:10])
        if not fac or not insp_date:
            continue
        name = (fac.get('establishment_name') or '').strip()
        if not name:
            continue

        violations = []
        pv = pfv = cv = 0
        for v in by_insp.get(str(a.get('inspection_id')), []):
            desc = ' '.join(x for x in [(v.get('violation_description') or '').strip(),
                                        (v.get('problem_description') or '').strip()] if x)
            if not desc:
                continue
            sev = DETROIT_SEVERITY.get((v.get('violation_type') or '').strip(), 'core')
            if sev == 'priority':
                pv += 1
            elif sev == 'priority_foundation':
                pfv += 1
            else:
                cv += 1
            cat, _, _ = classify_violation(desc)
            violations.append([cat, sev, desc[:500]])
        risk_score = max(0, 100 - (pv * 5) - (pfv * 2) - (cv * 1))

        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (fac.get('address') or '').strip().title(),
            'city': ((fac.get('city') or 'Detroit').strip().title()) or 'Detroit',
            'state': 'MI',
            'zip': str(fac.get('zip_code') or '')[:5],
            'latitude': fac.get('latitude'),
            'longitude': fac.get('longitude'),
            'inspection_date': insp_date,
            'original_score': None,  # Detroit publishes no numeric score
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': (a.get('inspection_type') or '').strip(),
            'results': (a.get('is_in_compliance') or '').strip()
                       if isinstance(a.get('is_in_compliance'), str) else '',
            'source': 'Detroit Open Data',
            'source_url': 'https://data.detroitmi.gov/datasets/food-service-establishment-inspections',
            'source_id': f"det_{a.get('inspection_id')}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Detroit: {len(results)} inspections")
    return results


# ─── SAN DIEGO COUNTY, CA (DEH sdfoodinfo.org JSON search) ───────────────────
# The county Department of Environmental Health's public inspection site is a
# jQuery front end over one form-encoded search endpoint that answers JSON
# (served as text/html). A date window returns every facility with at least
# one inspection of ANY type in the window, and each facility record carries
# its FULL inspection history (back to 2023-01) with violations inline, so no
# detail fetch is needed. Server time is dominated by the query, not the page
# size (a 1-row page of the whole county takes as long as a 1000-row one,
# about a minute), so always send a window and page at the site's own 1000.

SD_SEARCH_URL = 'https://sdfoodinfo.org/restaurants/search.htm'
SD_FACILITY_URL = 'https://sdfoodinfo.org/restaurants/restaurant_detail.htm?business_id={}'
SD_SITE_URL = 'https://www.sdfoodinfo.org/'
SD_HISTORY_START = '2023-01-01'   # the site holds no inspections before this
SD_PAGE = 1000                    # verified; a page is ~8 MB and 55-70 s
SD_TIMEOUT = 300
SD_RETRIES = 3
# Results come back sorted by distance from this origin (downtown San Diego,
# the site's own default). A fixed origin keeps the order, and so the page
# boundaries, stable from one page to the next (facilities without
# coordinates sort last, at ~7,800 miles from a 0,0 position).
SD_SORT_ORIGIN = ('32.715736', '-117.161087')
SD_HEADERS = {
    'User-Agent': CHROME_UA,
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'https://sdfoodinfo.org/restaurants/list_restaurants.html',
}
SD_INSPECTION_TYPES = ('Routine', 'Re-inspection')
SD_CLOSURE_STATUSES = ('Ordered Closed', 'Self Closed')
SD_CLOSURE_RISK_CAP = 55          # a closure grades F whatever the number says
_SD_ADDR_TAIL_RE = re.compile(r',\s*[^,]+,\s*CA\s+\d{5}(?:-\d{4})?\s*$', re.I)


def _sd_search_page(start_date, end_date, page_number, page_count=SD_PAGE):
    """POST one search page; returns (payload, seconds, bytes).

    Transient failures (timeouts, resets, 429/5xx, a non-JSON error page)
    are retried with backoff; anything else, or exhausting the retries,
    raises so the group records an error rather than shipping a partial
    window with an advanced freshness date."""
    data = [('name', ''), ('page_number', page_number), ('page_count', page_count),
            ('lat', SD_SORT_ORIGIN[0]), ('lng', SD_SORT_ORIGIN[1]),
            ('start_date', start_date), ('end_date', end_date)]
    last_err = None
    for attempt in range(1, SD_RETRIES + 1):
        t0 = time.time()
        try:
            r = requests.post(SD_SEARCH_URL, data=data, headers=SD_HEADERS, timeout=SD_TIMEOUT)
            r.raise_for_status()
            payload = r.json()   # served as text/html, but it is JSON
            if not isinstance(payload, dict) or not isinstance(payload.get('result'), list):
                raise ValueError(f'unexpected response shape: {str(payload)[:120]}')
            return payload, time.time() - t0, len(r.content)
        except (requests.RequestException, ValueError) as e:
            last_err = e
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            transient = status is None or status == 429 or status >= 500
            if not transient or attempt == SD_RETRIES or _past_deadline():
                break
            wait = 20 * attempt
            log.warning(f"  San Diego: page {page_number} failed ({e}) — retrying in "
                        f"{wait}s ({attempt}/{SD_RETRIES - 1})")
            time.sleep(wait)
    raise RuntimeError(f'San Diego search page {page_number} failed: {last_err}')


def _sd_title(s):
    """.title() for an ALL-CAPS source string, keeping possessives ("Joe's")."""
    s = ' '.join((s or '').split())
    if s.isupper():
        s = re.sub(r"'S\b", "'s", s.title())
    return s


def _sd_coord(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f and abs(f) <= 180 else None   # "0" means not geocoded


def _sd_split_address(address, city='', zip_code=''):
    """'1007 05TH AVE, SAN DIEGO, CA 92101' (plus the record's own city and
    zip fields) → ('1007 05TH AVE', 'SAN DIEGO', '92101')."""
    addr = ' '.join((address or '').split())
    city = ' '.join((city or '').split())
    zip_code = (zip_code or '').strip()
    street = addr
    tail = f", {city}, CA {zip_code}".lower()
    if city and zip_code and addr.lower().endswith(tail):
        street = addr[:-len(tail)]
    else:
        m = _SD_ADDR_TAIL_RE.search(addr)
        if m:
            street = addr[:m.start()]
            parts = [p.strip() for p in m.group(0).strip(', ').split(',')]
            city = city or parts[0]
            zip_code = zip_code or parts[-1].split()[-1]
    return street.strip(' ,'), city, zip_code[:5]


def _sd_tokens(s):
    return {w.rstrip('s') for w in re.findall(r'[a-z0-9]+', s.lower())}


def _sd_violation(v):
    """One source violation → [category, severity, description], or None.

    The description is the county's numbered item ("23. No rodents, insects,
    birds or animals") with the inspector's text appended only when it adds
    something (the site usually repeats the item's short name there).
    Severity follows the county's own flag: major → 'priority'; a minor item
    keeps the classifier's tier but is never 'priority'."""
    accela = ' '.join((v.get('violation_accela') or '').split()).rstrip(':').strip()
    name = ' '.join((v.get('violation') or '').split())
    text = ' '.join((v.get('violation_text') or '').split())
    major = ((v.get('major_violation') or '').strip().upper() == 'Y'
             or 'major' in (v.get('status') or '').lower())
    head = accela or name
    desc = head or text
    # The county's one-word finding ("Vermin", "Sewage") is appended only
    # for MAJOR citations. A minor citation keeps the bare item title (the
    # FDA form wording, as Chicago's and Omaha's do), so the text-forced F
    # for an active infestation fires on a county-flagged major vermin
    # finding and not on the hundreds of minor "Vermin" items a month.
    if major and head and text and text.lower() != head.lower():
        tok = _sd_tokens(text)
        if tok and len(tok & _sd_tokens(head)) * 2 < len(tok):
            desc = f'{head}: {text}'
    if not desc:
        return None
    cat, sev, _ = classify_violation(desc)
    if major:
        sev = 'priority'
    elif sev == 'priority':
        sev = 'priority_foundation'
    return [cat, sev, desc[:500]]


def _sd_facility_records(fac, since, seen_ids):
    """DineScores records for one facility's Routine/Re-inspection history
    on or after `since` (all of it when None). Returns (records, newest
    completed_date seen across ALL inspection types)."""
    street, city, zip5 = _sd_split_address(fac.get('address'), fac.get('city'), fac.get('zip'))
    name = _sd_title(fac.get('name'))
    if not name or not street:
        return [], ''
    street = _sd_title(street)
    city = _sd_title(city) if city else 'San Diego'
    lat, lng = _sd_coord(fac.get('lat')), _sd_coord(fac.get('long'))
    bid = str(fac.get('business_id') or '').strip()
    source_url = SD_FACILITY_URL.format(bid) if bid.isdigit() else SD_SITE_URL

    out, newest = [], ''
    for insp in fac.get('inspections') or []:
        date = sane_inspection_date(insp.get('completed_date'))
        if not date:
            continue
        newest = max(newest, date)
        itype = ' '.join((insp.get('type') or '').split())
        status = ' '.join((insp.get('status') or '').split())
        if itype not in SD_INSPECTION_TYPES or status == 'No Access':
            continue
        if since and date < since:
            continue
        iid = str(insp.get('inspection_id') or '').strip() or f'{bid}_{date}_{itype}'
        if iid in seen_ids:
            continue
        seen_ids.add(iid)

        violations = [x for x in (_sd_violation(v) for v in insp.get('violations') or []) if x]
        try:
            score = int(float(insp.get('score') or 0))
        except (TypeError, ValueError):
            score = 0
        if score > 0:
            original_score = score
            risk_score = max(0, min(100, score))
            pv = sum(1 for v in violations if v[1] == 'priority')
            pfv = sum(1 for v in violations if v[1] == 'priority_foundation')
            cv = len(violations) - pv - pfv
        else:
            original_score = None
            risk_score, pv, pfv, cv = calc_risk_score(violations)
        if status in SD_CLOSURE_STATUSES:
            risk_score = min(risk_score, SD_CLOSURE_RISK_CAP)

        grade = (insp.get('grade') or '').strip()
        results = [f'Grade {grade}'] if grade else []
        if status and status != 'Complete':
            results.append(status)

        out.append({
            'name': name,
            'address': street,
            'city': city,
            'state': 'CA',
            'zip': zip5,
            'latitude': lat,
            'longitude': lng,
            'inspection_date': date,
            'original_score': original_score,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': itype,
            'results': '; '.join(results),
            'source': 'San Diego County DEH',
            'source_url': source_url,
            'source_id': f'sd_{iid}',
            'metro': 'San Diego',
        })
    return out, newest


def fetch_san_diego(since_date=None, limit=None):
    """
    Fetch San Diego County inspections from the county DEH's public site
    (sdfoodinfo.org): ~17k permitted food facilities across the county's
    ~40 cities and unincorporated communities (San Diego, Chula Vista,
    Oceanside, Escondido, El Cajon, Carlsbad, ...), metro chip 'San Diego'.

    One record per Routine or Re-inspection inspection completed on or
    after since_date (the whole history back to 2023-01 when None). Site
    Investigation, Status Verification and Environmental visits are skipped
    (complaint follow-ups, permit checks and plan reviews, never scored),
    and so are 'No Access' visits (the inspector never got in; a record
    would read as a clean 100).

    Scoring: a scored inspection (score > 0; the county scores Routine
    inspections 0-100 and grades them A >= 90 / B 80-89 / C 70-79) ships its
    official score as both original_score and risk_score. Unscored ones
    (re-inspections, a few routines) are computed from the violations with
    calc_risk_score. Severity follows the county's own flag: a major
    violation (major_violation Y / status '... - Major') is 'priority'; a
    minor one keeps the classifier's tier but is never 'priority'. An
    inspection whose status is 'Ordered Closed' or 'Self Closed' has its
    risk_score capped at 55 so the closure grades F regardless of the
    number: a closure carries score 0 and typically one or two major items,
    which would otherwise compute to a 90+.

    The window is sent server-side (start_date/end_date select facilities
    with an inspection in it) and applied again per inspection, since each
    facility comes back with its full history.
    """
    log.info(f"Fetching San Diego County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    start = since or SD_HISTORY_START
    end = datetime.now().strftime('%Y-%m-%d')
    page_count = min(SD_PAGE, max(int(limit), 1)) if limit else SD_PAGE

    results, seen, newest = [], set(), ''
    facilities, page, bad = 0, 1, 0
    while True:
        payload, dt, nbytes = _sd_search_page(start, end, page, page_count)
        rows = payload.get('result') or []
        try:
            total = int(payload.get('total_count'))
        except (TypeError, ValueError):
            total = None
        facilities += len(rows)
        for fac in rows:
            try:
                recs, nw = _sd_facility_records(fac, since, seen)
            except Exception as e:
                bad += 1
                if bad <= 5:
                    log.warning(f"  San Diego: skipping facility "
                                f"{fac.get('business_id') if isinstance(fac, dict) else fac!r}: {e}")
                continue
            results.extend(recs)
            newest = max(newest, nw)
        log.info(f"  San Diego: page {page} — {len(rows)} facilities in {dt:.0f}s "
                 f"({nbytes / 1e6:.1f} MB); {facilities}"
                 f"{f'/{total}' if total is not None else ''} facilities, "
                 f"{len(results)} inspections so far")
        if (not rows or len(rows) < page_count
                or (total is not None and facilities >= total)
                or (limit and len(results) >= limit)):
            break
        if _past_deadline():
            raise RuntimeError(f'fetch deadline reached after page {page} '
                               f'({facilities}/{total} facilities)')
        page += 1
    if bad:
        log.warning(f"  San Diego: {bad} facility records could not be parsed")
    if limit:
        results = results[:limit]
    _note_source_lag('San Diego County', newest)
    log.info(f"San Diego County: {len(results)} inspections from {facilities} facilities "
             f"(source newest {newest or 'n/a'})")
    return results


# ─── SACRAMENTO COUNTY, CA (county ArcGIS, MHD-backed) ───────────────────────
# Layer 0 = facilities with point geometry; layer 1 = inspection/violation
# history rows. No numeric score — the county publishes categorical results
# (PASS / MINOR VIOLATIONS / CONDITIONAL PASS / CRITICAL VIOLATIONS / CLOSED),
# mapped onto the risk scale below; violation text (when present) feeds the
# chips/summaries and the pest auto-F.

SACRAMENTO_BASE = ('https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/'
                   'services/Food_Inspections/FeatureServer')
SACRAMENTO_RESULT_RISK = {
    'PASS': 100, 'IN COMPLIANCE': 100, 'MINOR VIOLATIONS': 85,
    'CONDITIONAL PASS': 70, 'CRITICAL VIOLATIONS': 60,
    'SUSPENSION': 45, 'CLOSED': 40,
}


def _sac_parse_address(combined):
    """'5321 Date Ave, Sacramento 95841-2512' → (street, city, zip)."""
    m = re.match(r'^(.*?),\s*(.*?)\s+(\d{5})(?:-\d{4})?\s*$', combined or '')
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3)
    return (combined or '').strip(), 'Sacramento', ''


def fetch_sacramento(since_date=None, limit=None):
    log.info(f"Fetching Sacramento County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'

    fac_rows = _arcgis_query_all(
        f'{SACRAMENTO_BASE}/0', out_fields='Facility_ID,Facility_Name,Facility_Address')
    facilities = {}
    for f in fac_rows:
        a = f.get('attributes', {})
        geom = f.get('geometry') or {}
        if a.get('Facility_ID'):
            a['_lng'], a['_lat'] = geom.get('x'), geom.get('y')
            facilities[str(a['Facility_ID'])] = a
    log.info(f"  Sacramento: {len(facilities)} facilities")

    rows = _arcgis_query_all(
        f'{SACRAMENTO_BASE}/1', where=f"Inspection_Date >= DATE '{since}'",
        out_fields='Facility_ID,Inspection_Type,Inspection_Result,'
                   'Inspection_Date,Violation_Description')
    log.info(f"  Sacramento: {len(rows)} inspection/violation rows since {since}")

    by_insp = defaultdict(list)
    for f in rows:
        a = f.get('attributes', {})
        ts = a.get('Inspection_Date')
        if not a.get('Facility_ID') or not ts:
            continue
        if isinstance(ts, (int, float)):
            d = datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d')
        else:
            d = str(ts)[:10]
        by_insp[(str(a['Facility_ID']), d)].append(a)

    results = []
    for (fid, d), group in by_insp.items():
        fac = facilities.get(fid)
        insp_date = sane_inspection_date(d)
        if not fac or not insp_date:
            continue
        name = (fac.get('Facility_Name') or '').strip()
        if not name:
            continue
        result = next(((a.get('Inspection_Result') or '').strip().upper()
                       for a in group if a.get('Inspection_Result')), '')
        risk_score = SACRAMENTO_RESULT_RISK.get(result, 70)

        violations = []
        for a in group:
            desc = (a.get('Violation_Description') or '').strip()
            if desc:
                violations.append(classify_violation(desc[:500]))
        _, pv, pfv, cv = calc_risk_score(violations)

        street, city, zip_code = _sac_parse_address(fac.get('Facility_Address'))
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': street.title() if street.isupper() else street,
            'city': city.title() if city.isupper() else city,
            'state': 'CA',
            'zip': zip_code,
            'latitude': fac.get('_lat'),
            'longitude': fac.get('_lng'),
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': risk_score,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': [list(v) for v in violations],
            'inspection_type': next(((a.get('Inspection_Type') or '').strip()
                                     for a in group if a.get('Inspection_Type')), ''),
            'results': result.title(),
            'source': 'Sacramento County Open Data',
            'source_url': 'https://data.saccounty.gov',
            'source_id': f'sac_{fid}_{insp_date}',
            'metro': 'Sacramento',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Sacramento County: {len(results)} inspections")
    return results


# ─── SANTA CLARA COUNTY, CA (San Jose; county LIVES feed on Socrata) ─────────
# The county Department of Environmental Health publishes its food
# inspections in the LIVES layout on data.sccgov.org, refreshed daily over a
# rolling ~two years, as three tables that are joined in memory here:
#   INSPECTIONS  one row per inspection: the official 0-100 score (present
#                on ~81% of rows — follow-ups mostly carry none), the placard
#                colour (G green / Y yellow conditional / R red closure) and
#                the inspection type. The id column really is spelled
#                `inpsection_id`, and `date` is TEXT 'YYYYMMDD' rather than
#                a Socrata date, so the window compares strings.
#   VIOLATIONS   one row per cited item with the county's own `critical`
#                flag; no date column, joins on the inspection id.
#   BUSINESS     name/address/city/zip + coordinates for ~9.7k facilities.
# No public per-facility page is reachable (the county lookup sits behind a
# Cloudflare challenge), so source_url is the dataset page.

SCC_INSPECTIONS_URL = 'https://data.sccgov.org/resource/2u2d-8jej.json'
SCC_VIOLATIONS_URL = 'https://data.sccgov.org/resource/wkaa-4ccv.json'
SCC_BUSINESS_URL = 'https://data.sccgov.org/resource/vuw7-jmjk.json'
SCC_SOURCE_PAGE = 'https://data.sccgov.org/Health/SCC_DEH_Food_Data_INSPECTIONS/2u2d-8jej'
SCC_PLACARDS = {'G': 'Green placard', 'Y': 'Yellow placard (conditional pass)',
                'R': 'Red placard (closed)'}
# A red placard closes the facility. The county's score measures overall
# code compliance while the placard tracks major violations, so closures
# routinely carry scores in the 70s-80s; cap them so a closure grades F.
SCC_RED_PLACARD_CAP = 55       # a closure grades F whatever the numeric score
SCC_YELLOW_PLACARD_CAP = 79    # a conditional pass grades no better than C
SCC_ID_CHUNK = 100             # inspection ids per `in (...)` violations request
SCC_BULK_VIOLATIONS_DAYS = 60  # wider windows download the whole violations table
SCC_PAGE = 50000
# The output paths keep ONE inspection per restaurant per day (first seen
# wins), so when a business has two inspections on one day the scored
# routine inspection must be emitted before its unscored follow-up, and
# between otherwise-equal visits the worse placard (a closure) comes first.
SCC_TYPE_RANK = {'ROUTINE INSPECTION': 0, 'RISK FACTOR INSPECTION': 1,
                 'FOLLOW-UP INSPECTION': 2}
SCC_PLACARD_RANK = {'R': 0, 'Y': 1, 'G': 2}
SCC_CITY_FIXES = {'MOUNTIAN VIEW': 'MOUNTAIN VIEW'}   # source typo on a few facilities
# "[CA] ..." introduces the inspector's corrective-action text (and anything
# after it, e.g. "[COS]"); only the observation before it describes what was
# found, so that is what gets classified and shown.
SCC_CORRECTIVE_TAIL_RE = re.compile(r'\s*\[CA\].*$', re.S)


def _scc_window_start(since_date):
    """'YYYY-MM-DD[T00:00:00]' / None -> the feed's 'YYYYMMDD' text / None."""
    if not since_date:
        return None
    s = str(since_date)[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').strftime('%Y%m%d')
    except ValueError:
        log.warning(f"  Santa Clara: unusable since_date {since_date!r} — fetching everything")
        return None


def _scc_iso_date(yyyymmdd):
    s = str(yyyymmdd or '').strip()
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}' if len(s) == 8 and s.isdigit() else ''


def _scc_clean(s):
    return ' '.join(str(s or '').split())


def _scc_proper(s):
    s = _scc_clean(s)
    return s.title() if s.isupper() else s


def _scc_violation_text(description, comment):
    """'<item description>: <observation>' with the corrective-action tail dropped."""
    desc = _scc_clean(description)
    obs = _scc_clean(SCC_CORRECTIVE_TAIL_RE.sub('', comment or ''))
    if desc and obs:
        return f'{desc}: {obs}'
    return desc or obs


def _scc_violation(row):
    """[category, severity, text] for one VIOLATIONS row, or None if blank.
    The county's own `critical` flag anchors severity: critical -> priority;
    a non-critical item keeps the classifier's tier but never above
    priority_foundation."""
    text = _scc_violation_text(row.get('description'), row.get('violation_comment'))
    if not text:
        return None
    cat, clf_sev, _ = classify_violation(text)
    crit = row.get('critical')
    if isinstance(crit, str):
        crit = crit.strip().lower() in ('true', 't', '1', 'yes', 'y')
    if crit:
        sev = 'priority'
    elif clf_sev == 'priority':
        sev = 'priority_foundation'
    else:
        sev = clf_sev
    return [cat, sev, text[:500]]


def _scc_feed_newest_date():
    """The feed's own newest inspection date (ISO), '' if the probe fails."""
    try:
        r = requests.get(SCC_INSPECTIONS_URL, params={'$select': 'max(date)'},
                         headers=_socrata_headers(), timeout=60)
        r.raise_for_status()
        rows = r.json()
        return _scc_iso_date((rows[0] if rows else {}).get('max_date'))
    except (requests.RequestException, ValueError, IndexError, AttributeError) as e:
        log.warning(f"  Santa Clara: newest-date probe failed: {e}")
        return ''


def _scc_fetch_violations(insp_ids, window_days):
    """VIOLATIONS rows for the given inspection ids, grouped by id. Narrow
    windows ask for exactly those ids (100 per request); an open or >60-day
    window with more than a few hundred inspections pulls the whole ~65k-row
    table in 50k pages instead."""
    ids = sorted(i for i in insp_ids if i)
    by_insp = defaultdict(list)
    if not ids:
        return by_insp
    bulk = ((window_days is None or window_days > SCC_BULK_VIOLATIONS_DAYS)
            and len(ids) > 3 * SCC_ID_CHUNK)
    if bulk:
        rows = _socrata_fetch_pages(
            SCC_VIOLATIONS_URL, {'$order': ':id', '$limit': SCC_PAGE, '$offset': 0}, None)
        log.info(f"  Santa Clara: {len(rows)} violation rows (whole table)")
    else:
        rows = []
        for k in range(0, len(ids), SCC_ID_CHUNK):
            chunk = ids[k:k + SCC_ID_CHUNK]
            quoted = ','.join("'" + i.replace("'", "''") + "'" for i in chunk)
            rows.extend(_socrata_fetch_pages(
                SCC_VIOLATIONS_URL,
                {'$where': f'inspection_id in ({quoted})', '$order': ':id',
                 '$limit': SCC_PAGE, '$offset': 0}, None))
        log.info(f"  Santa Clara: {len(rows)} violation rows for {len(ids)} inspections "
                 f"({(len(ids) + SCC_ID_CHUNK - 1) // SCC_ID_CHUNK} requests)")
    wanted = set(ids)
    for v in rows:
        iid = v.get('inspection_id')
        if iid in wanted:
            by_insp[iid].append(v)
    return by_insp


def fetch_santa_clara(since_date=None, limit=None):
    """
    Fetch Santa Clara County (San Jose, Sunnyvale, Santa Clara, Mountain
    View, Palo Alto, Milpitas, Cupertino, Gilroy, ...) food inspections from
    the county's LIVES feed on data.sccgov.org. One record per inspection:
    risk_score is the county's OFFICIAL 0-100 score when published
    (original_score = that score); an unscored inspection (mostly follow-ups)
    is scored from its violations (5/2/1) with original_score None.
    Violation severity follows the county's own `critical` flag. The placard
    is the county's stated tier and the grade never contradicts it: a RED
    placard means the facility was closed, so risk_score is capped at
    SCC_RED_PLACARD_CAP (55, an F), and a YELLOW placard (conditional pass,
    major violations found) is capped at SCC_YELLOW_PLACARD_CAP (79, a C),
    regardless of the numeric score — the score measures overall code
    compliance while the placard tracks major violations, and a closed or
    conditionally passed restaurant must not show as an A or B. Inspections
    whose business is missing from the BUSINESS table are skipped (counted
    in the log).
    """
    log.info(f"Fetching Santa Clara County data (since={since_date}, limit={limit})")
    start = _scc_window_start(since_date)
    window_days = None
    if start:
        window_days = (datetime.now() - datetime.strptime(start, '%Y%m%d')).days

    params = {
        '$order': 'date DESC, inpsection_id',
        '$limit': min(limit, SCC_PAGE) if limit else SCC_PAGE,
        '$offset': 0,
    }
    if start:
        params['$where'] = f"date >= '{start}'"
    insp_rows = _socrata_fetch_pages(SCC_INSPECTIONS_URL, params, limit)
    log.info(f"  Santa Clara: {len(insp_rows)} inspection rows"
             + (f" since {_scc_iso_date(start)}" if start else ''))

    # The feed's own newest row explains a STALE flag when the county itself
    # is behind; fall back to the newest row fetched if the probe fails.
    newest = _scc_feed_newest_date() or max(
        (_scc_iso_date(r.get('date')) for r in insp_rows), default='')
    _note_source_lag('Santa Clara County', newest)
    if not insp_rows:
        return []

    biz_rows = _socrata_fetch_pages(
        SCC_BUSINESS_URL, {'$order': 'business_id', '$limit': SCC_PAGE, '$offset': 0}, None)
    businesses = {b['business_id']: b for b in biz_rows if b.get('business_id')}
    log.info(f"  Santa Clara: {len(businesses)} businesses")

    insp_ids = {r.get('inpsection_id') for r in insp_rows if r.get('inpsection_id')}
    viols_by_insp = _scc_fetch_violations(insp_ids, window_days)

    def _order(r):
        d = str(r.get('date') or '')
        return (-(int(d) if d.isdigit() else 0),
                SCC_TYPE_RANK.get(_scc_clean(r.get('type')).upper(), 9),
                0 if str(r.get('score') or '').strip() else 1,
                SCC_PLACARD_RANK.get(_scc_clean(r.get('result')).upper(), 9),
                str(r.get('inpsection_id') or ''))
    insp_rows.sort(key=_order)

    results = []
    missing_biz = 0
    seen_ids = set()
    for row in insp_rows:
        insp_id = _scc_clean(row.get('inpsection_id'))
        if not insp_id or insp_id in seen_ids:
            continue
        seen_ids.add(insp_id)
        biz = businesses.get(row.get('business_id'))
        if not biz:
            missing_biz += 1
            continue
        insp_date = sane_inspection_date(_scc_iso_date(row.get('date')))
        name = _scc_proper(biz.get('name'))
        if not insp_date or not name:
            continue

        violations = [v for v in (_scc_violation(v) for v in viols_by_insp.get(insp_id, ()))
                      if v]
        computed, pv, pfv, cv = calc_risk_score([tuple(v) for v in violations])
        try:
            score = int(round(float(row.get('score'))))
        except (TypeError, ValueError):
            score = None
        risk = max(0, min(100, score)) if score is not None else computed
        placard = _scc_clean(row.get('result')).upper()
        if placard == 'R':
            risk = min(risk, SCC_RED_PLACARD_CAP)
        elif placard == 'Y':
            risk = min(risk, SCC_YELLOW_PLACARD_CAP)

        city_raw = _scc_clean(biz.get('city')).upper()
        city = SCC_CITY_FIXES.get(city_raw, city_raw).title() or 'San Jose'
        try:
            lat, lng = float(biz.get('latitude')), float(biz.get('longitude'))
            if not lat or not lng:
                lat = lng = None
        except (TypeError, ValueError):
            lat = lng = None

        results.append({
            'name': name,
            'address': _scc_proper(biz.get('address')),
            'city': city,
            'state': 'CA',
            'zip': _scc_clean(biz.get('postal_code'))[:5],
            'latitude': lat,
            'longitude': lng,
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': risk,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': _scc_clean(row.get('type')),
            'results': SCC_PLACARDS.get(placard, ''),
            'source': 'Santa Clara County Open Data',
            'source_url': SCC_SOURCE_PAGE,
            'source_id': f'scc_{insp_id}',
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    if missing_biz:
        log.warning(f"  Santa Clara: {missing_biz} inspections skipped — business id not "
                    f"in the BUSINESS table")
    log.info(f"Santa Clara County: {len(results)} inspections")
    return results


# ─── SOUTH CAROLINA (statewide, SC DPH ArcGIS) ───────────────────────────────
# Two-layer join like Wake: a points layer with coordinates and an
# inspections table with dates and POSTED_GRADE (A/B/C — no numeric score
# and no violation text; the full report is only a PDF). Grades map onto
# representative risk scores so the A/B/C/F scheme carries through.

SC_BASE = ('https://services5.arcgis.com/G4BLIH7rTQoIjCFv/arcgis/rest/'
           'services/Restaurants/FeatureServer')
SC_GRADE_RISK = {'A': 95, 'B': 85, 'C': 75}


def fetch_sc(since_date=None, limit=None):
    log.info(f"Fetching South Carolina data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'

    fac_rows = _arcgis_query_all(
        f'{SC_BASE}/0', out_fields='PERMIT_ID,FACILITY,ADDRESS,CITY,STATE,ZIPCODE',
        page_size=2000)
    facilities = {}
    for f in fac_rows:
        a = f.get('attributes', {})
        geom = f.get('geometry') or {}
        if a.get('PERMIT_ID'):
            a['_lng'], a['_lat'] = geom.get('x'), geom.get('y')
            facilities[str(a['PERMIT_ID']).strip()] = a
    log.info(f"  SC: {len(facilities)} facilities")

    insp_rows = _arcgis_query_all(
        f'{SC_BASE}/4', where=f"INSP_DATE >= DATE '{since}'",
        out_fields='PERMIT_ID,POSTED_GRADE,INSP_DATE,AC_CODE,Active',
        page_size=2000)
    log.info(f"  SC: {len(insp_rows)} inspections since {since}")

    results = []
    for f in insp_rows:
        a = f.get('attributes', {})
        fac = facilities.get(str(a.get('PERMIT_ID') or '').strip())
        ts = a.get('INSP_DATE')
        grade = (a.get('POSTED_GRADE') or '').strip().upper()
        if not fac or not ts or grade not in SC_GRADE_RISK:
            continue
        insp_date = sane_inspection_date(
            datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d'))
        name = (fac.get('FACILITY') or '').strip()
        if not insp_date or not name:
            continue
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (fac.get('ADDRESS') or '').strip().title(),
            'city': ((fac.get('CITY') or '').strip().title()) or 'South Carolina',
            'state': 'SC',
            'zip': str(fac.get('ZIPCODE') or '')[:5],
            'latitude': fac.get('_lat'),
            'longitude': fac.get('_lng'),
            'inspection_date': insp_date,
            'original_score': None,  # SC publishes only letter grades
            'risk_score': SC_GRADE_RISK[grade],
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': (a.get('AC_CODE') or '').strip(),
            'results': grade,
            'source': 'SC DPH Food Grades',
            'source_url': 'https://dataviz.dph.sc.gov/foodgrades/',
            'source_id': f"sc_{a.get('PERMIT_ID')}_{insp_date}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"South Carolina: {len(results)} inspections")
    return results


# ─── OMAHA / DOUGLAS COUNTY, NE (county ArcGIS + i2g inspection PDFs) ────────
# Douglas County Health Department publishes every inspection since its i2g
# inspection system went live (Dec 2025) as a point layer on ArcGIS Online:
# one row per inspection with the establishment, the county's letter RATING
# (A-F) and a link to the public inspection-report PDF. No numeric score is
# published. The PDF (dompdf, one page: the FDA Food Code inspection form)
# carries the inspector's TOTAL P / PF / C violation counts and the 56 form
# items marked IN/OUT/N/O/N/A. The fetcher reads it with the standard
# library alone (zlib + the Tj/TJ text operators) and scores
# 100 - 5P - 2PF - C from the totals. An inspection whose PDF cannot be
# fetched or parsed is DROPPED, never shipped as violation-free; the weekly
# lookback refetches it. The layer's Score/Status/Match_type/Match_addr/
# Addr_type fields are the county GEOCODER's output, never an inspection
# score. The companion MobileFoodInspections table (mobile units, no
# geometry, a separate PDF endpoint) is not read.

OMAHA_LAYER = ('https://services.arcgis.com/pDAi2YK0L0QxVJHj/arcgis/rest/services/'
               'Restaurant_Inspections/FeatureServer/14')
OMAHA_SOURCE = 'Douglas County Health Dept'
# Errors the county's PDF endpoint returns for specific records on every
# request (a slash in the establishment name; a missing record).
OMAHA_PERMANENT_500S = ('filename and the fallback cannot contain',
                        'Attempt to read property')
OMAHA_DETAIL_WORKERS = int(os.environ.get('OMAHA_DETAIL_WORKERS', '3'))
OMAHA_DETAIL_DELAY = float(os.environ.get('OMAHA_DETAIL_DELAY', '0.3'))
OMAHA_DETAIL_TIMEOUT = 60
OMAHA_FORM_MARKS = frozenset({'IN', 'OUT', 'NO', 'N/O', 'NA', 'N/A'})
# The form's 56 items by the Food Code tier of each item's principal
# provision (FDA Form 3-A / 2017 Food Code). Items 1-29 are the risk factors
# and interventions, 30-56 the good retail practices; the form itself does
# not print a tier per item, so this map seeds the severity of an OUT item
# and _oma_item_severities reconciles it with the inspector's totals.
OMAHA_P_ITEMS = frozenset({3, 4, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21,
                           22, 24, 26, 27, 28, 29, 30, 31, 52})
OMAHA_PF_ITEMS = frozenset({1, 5, 10, 14, 23, 25, 32, 33, 34, 36, 48, 50, 51})
_OMAHA_TOTALS_RE = re.compile(
    r'TOTAL\s*P\s*:\s*(\d+)\s*TOTAL\s*PF\s*:\s*(\d+)\s*TOTAL\s*C\s*:\s*(\d+)', re.I)
_PDF_TOKEN_RE = re.compile(
    rb'(?P<ws>[\s\x00]+)|(?P<comment>%[^\r\n]*)|(?P<str>\()|(?P<dict><<|>>)|'
    rb'(?P<hex><[0-9A-Fa-f\s]*>)|(?P<arr>[\[\]])|(?P<name>/[^\s/\[\]()<>{}%]*)|'
    rb'(?P<num>[-+]?(?:\d+\.?\d*|\.\d+))|(?P<op>[^\s/\[\]()<>{}%]+)|(?P<other>.)', re.S)
_PDF_ESCAPES = {0x6e: 0x0a, 0x72: 0x0d, 0x74: 0x09, 0x62: 0x08, 0x66: 0x0c}


def _pdf_literal(data, i):
    """Decode the PDF literal string that opens at data[i] == '('.
    Returns (text, index after the closing paren)."""
    n = len(data)
    buf = bytearray()
    depth = 0
    j = i
    while j < n:
        ch = data[j]
        if ch == 0x5c:                      # backslash escape
            j += 1
            if j >= n:
                break
            e = data[j]
            if e in _PDF_ESCAPES:
                buf.append(_PDF_ESCAPES[e]); j += 1
            elif 0x30 <= e <= 0x37:          # \ddd octal
                k, code = j, 0
                while k < n and k < j + 3 and 0x30 <= data[k] <= 0x37:
                    code = code * 8 + (data[k] - 0x30); k += 1
                buf.append(code & 0xff); j = k
            elif e in (0x0d, 0x0a):          # line continuation
                j += 1
                if e == 0x0d and j < n and data[j] == 0x0a:
                    j += 1
            else:
                buf.append(e); j += 1
            continue
        if ch == 0x28:                      # (
            depth += 1
            if depth > 1:
                buf.append(ch)
        elif ch == 0x29:                    # )
            depth -= 1
            if depth == 0:
                j += 1
                break
            buf.append(ch)
        else:
            buf.append(ch)
        j += 1
    return bytes(buf).decode('cp1252', errors='replace'), j


def _pdf_tokens(data):
    """Yield (kind, value) tokens of a PDF content stream: 'str' (decoded
    text), 'num' (float), 'name', 'arr' ('[' or ']') and 'op' (operator)."""
    i, n = 0, len(data)
    while i < n:
        m = _PDF_TOKEN_RE.match(data, i)
        kind = m.lastgroup
        if kind == 'str':
            text, i = _pdf_literal(data, i)
            yield 'str', text
            continue
        i = m.end()
        if kind in ('ws', 'comment', 'dict', 'other'):
            continue
        if kind == 'hex':
            hexs = re.sub(rb'\s', b'', m.group()[1:-1])
            if len(hexs) % 2:
                hexs += b'0'
            try:
                yield 'str', bytes.fromhex(hexs.decode('ascii')).decode('cp1252', errors='replace')
            except ValueError:
                pass
        elif kind == 'num':
            try:
                yield 'num', float(m.group())
            except ValueError:
                yield 'op', m.group().decode('latin-1')
        elif kind == 'arr':
            yield 'arr', m.group().decode('latin-1')
        elif kind == 'name':
            yield 'name', m.group().decode('latin-1')
        else:
            yield 'op', m.group().decode('latin-1')


def _pdf_text_items(stream):
    """[(x, y, text)] for every BT..ET block of a content stream, positioned
    by its first Td/TD/Tm (dompdf emits one block per string)."""
    items = []
    operands = []
    array = None
    x = y = 0.0
    cur = None
    for kind, val in _pdf_tokens(stream):
        if kind == 'arr':
            if val == '[':
                array = []
            else:
                operands.append(('arr', array or [])); array = None
            continue
        if kind != 'op':
            if array is not None:
                if kind == 'str':
                    array.append(val)
            else:
                operands.append((kind, val))
            continue
        if val == 'BT':
            x = y = 0.0; cur = None
        elif val == 'ET':
            if cur is not None and cur[2].strip():
                items.append((cur[0], cur[1], cur[2]))
            cur = None
        elif val in ('Td', 'TD'):
            nums = [v for k, v in operands if k == 'num']
            if len(nums) >= 2:
                x += nums[-2]; y += nums[-1]
        elif val == 'Tm':
            nums = [v for k, v in operands if k == 'num']
            if len(nums) >= 6:
                x, y = nums[-2], nums[-1]
        elif val in ('Tj', 'TJ', "'", '"'):
            text = ''
            for k, v in operands:
                if k == 'str':
                    text += v
                elif k == 'arr':
                    text += ''.join(v)
            if cur is None:
                cur = [x, y, '']
            cur[2] += text
        operands = []
    return items


def _oma_pdf_text_rows(pdf_bytes, y_tolerance=1.5):
    """Positioned text of a PDF as rows: [(stream_index, y, [(x, text), ...])],
    top-down within each content stream, cells left-to-right. Image streams
    are skipped; Flate streams are inflated with zlib (stdlib only)."""
    rows = []
    stream_no = 0
    for m in re.finditer(rb'stream\r?\n', pdf_bytes):
        start = m.end()
        end = pdf_bytes.find(b'endstream', start)
        if end < 0:
            break
        obj_start = pdf_bytes.rfind(b'obj', 0, m.start())
        header = pdf_bytes[obj_start if obj_start >= 0 else 0:m.start()]
        if b'/Image' in header or b'/XObject' in header or b'/FontFile' in header:
            continue
        raw = pdf_bytes[start:end]
        if b'/FlateDecode' in header:
            try:
                data = zlib.decompress(raw)
            except zlib.error:
                try:
                    data = zlib.decompressobj().decompress(raw)
                except zlib.error:
                    continue
        elif re.search(rb'/Filter\s*[/\[]', header):
            continue                        # another filter: not a text stream we can read
        else:
            data = raw
        if b'BT' not in data:
            continue
        items = _pdf_text_items(data)
        items.sort(key=lambda t: (-t[1], t[0]))
        current = None
        for x, y, text in items:
            if current is None or abs(current[1] - y) > y_tolerance:
                current = (stream_no, y, [])
                rows.append(current)
            current[2].append((x, text))
        stream_no += 1
    for _, _, cells in rows:
        cells.sort()
    return rows


def _oma_value_below(rows, label_re, max_dy=16, max_dx=30):
    """Text of the cell directly under the first cell matching label_re
    (the i2g header boxes print a label with its value on the next line)."""
    for idx, (page, y, cells) in enumerate(rows):
        for x, text in cells:
            if not re.fullmatch(label_re, text.strip()):
                continue
            for page2, y2, cells2 in rows[idx + 1:]:
                if page2 != page or y - y2 > max_dy:
                    break
                for x2, text2 in cells2:
                    if abs(x2 - x) <= max_dx and text2.strip():
                        return text2.strip()
            return None
    return None


def _oma_parse_report(pdf_bytes):
    """Read an i2g inspection-report PDF. Returns a dict with the totals
    (p, pf, c), the county's rating letter, the purpose line, the
    inspection date (MM/DD/YYYY as printed) and the form items marked OUT as
    [(item_number, title)]. Raises ValueError when the form is not found."""
    rows = _oma_pdf_text_rows(pdf_bytes)
    totals = None
    totals_idx = None
    for idx, (page, y, cells) in enumerate(rows):
        m = _OMAHA_TOTALS_RE.search(' '.join(t for _, t in cells))
        if m:
            totals = tuple(int(g) for g in m.groups())
            totals_idx = idx
            break
    if totals is None:
        raise ValueError('no TOTAL P/PF/C line in the inspection PDF')
    out_items = []
    seen = set()
    for page, y, cells in rows[totals_idx + 1:]:
        num = mark = None
        for x, text in cells:
            s = text.strip()
            if re.fullmatch(r'\d{1,2}', s) and 1 <= int(s) <= 56:
                num, mark = int(s), None
                continue
            if num is None:
                continue
            if mark is None and s.upper() in OMAHA_FORM_MARKS and len(s) <= 3:
                mark = s.upper()
                continue
            if mark == 'OUT' and num not in seen:
                seen.add(num)
                out_items.append((num, ' '.join(s.split())))
            num = mark = None
    out_items.sort()
    rating = _oma_value_below(rows, r'Rating\s*:?')
    purpose = _oma_value_below(rows, r'Purpose\s*:?')
    date = _oma_value_below(rows, r'Inspection Date\s*:?')
    return {'totals': totals, 'rating': rating or None, 'purpose': purpose or '',
            'date': date or '', 'out_items': out_items}


def _oma_item_severities(out_items, totals):
    """Severity per OUT form item: seeded by the item's Food Code tier and
    reconciled with the inspector's TOTAL P/PF/C so no tier the totals say
    was not cited appears, and no tier carries more items than its total
    (an item can hold several violations, so a tier may carry fewer).
    Returns {item_number: severity}."""
    p, pf, c = totals
    sev = {}
    for num, _ in out_items:
        sev[num] = ('priority' if num in OMAHA_P_ITEMS else
                    'priority_foundation' if num in OMAHA_PF_ITEMS else 'core')

    def members(tier):
        return sorted(n for n in sev if sev[n] == tier)

    # Too many P items: the highest-numbered become PF; too many PF: core.
    for tier, total, lower in (('priority', p, 'priority_foundation'),
                               ('priority_foundation', pf, 'core')):
        for n in members(tier)[total:]:
            sev[n] = lower
    # Too many core items: the lowest-numbered move up to a tier with room.
    core = members('core')
    for n in core[:max(0, len(core) - c)]:
        if len(members('priority_foundation')) < pf:
            sev[n] = 'priority_foundation'
        elif len(members('priority')) < p:
            sev[n] = 'priority'
    return sev


def _oma_fetch_pdf(url, session=None, info=None):
    """Download one inspection PDF: bytes, or None after one retry. `info`
    (a dict) receives the last HTTP status ('status'; None after a network
    error), so the caller can tell the county's deterministic HTTP 500s
    (its PDF endpoint crashes on some establishments — a slash in the name,
    a missing record) from failures worth retrying next run."""
    client = session or requests
    for attempt in (1, 2):
        try:
            r = client.get(url, headers={'User-Agent': CHROME_UA},
                           timeout=OMAHA_DETAIL_TIMEOUT)
            if info is not None:
                info['status'] = r.status_code
                info['body'] = ((r.content or b'')[:600].decode('utf-8', 'replace')
                                if r.status_code != 200 else '')
            if r.status_code == 200 and r.content[:5] == b'%PDF-':
                return r.content
            log.debug(f"  Omaha PDF {url}: HTTP {r.status_code}, "
                      f"{len(r.content)} bytes (attempt {attempt})")
        except requests.RequestException as e:
            if info is not None:
                info['status'] = None
            log.debug(f"  Omaha PDF {url}: {e} (attempt {attempt})")
        if attempt == 1:
            time.sleep(2)
    return None


def _oma_layer_newest():
    """Newest inspection_date in the layer (for the lag note when the
    window itself comes back empty), or ''."""
    try:
        r = requests.get(OMAHA_LAYER + '/query', params={
            'where': '1=1', 'outFields': 'inspection_date', 'returnGeometry': 'false',
            'orderByFields': 'inspection_date DESC', 'resultRecordCount': 1, 'f': 'json',
        }, headers={'User-Agent': CHROME_UA}, timeout=60)
        feats = r.json().get('features') or []
        return str(feats[0]['attributes'].get('inspection_date') or '')[:10] if feats else ''
    except Exception as e:  # noqa: BLE001 - the note is best-effort
        log.debug(f"  Omaha: newest-date probe failed: {e}")
        return ''


def _oma_clean(text):
    """Collapse whitespace; title-case an ALL CAPS value."""
    s = ' '.join(str(text or '').split())
    return s.title() if s.isupper() else s


def fetch_omaha(since_date=None, limit=None):
    """
    Fetch Omaha / Douglas County, NE inspections: the county's ArcGIS layer
    (one row per inspection, letter rating, coordinates, PDF link), then each
    inspection's report PDF for the TOTAL P/PF/C counts and the OUT items.
    original_score is None (the county publishes a letter, not a number);
    risk_score = 100 - 5P - 2PF - C from the inspector's totals. Inspections
    whose PDF cannot be read are dropped.
    """
    log.info(f"Fetching Omaha / Douglas County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    where = f"inspection_date >= DATE '{since}'" if since else '1=1'
    rows = _arcgis_query_all(
        OMAHA_LAYER, where=where,
        out_fields='inspection_id,inspection_date,est_name,est_address,address_2,'
                   'est_city,est_state,est_zip,firm_number,rating,downloadDetailsUrl')
    log.info(f"  Omaha: {len(rows)} inspection rows since {since or 'the start of the feed'}")
    newest = max((str(f.get('attributes', {}).get('inspection_date') or '')[:10]
                  for f in rows), default='') or _oma_layer_newest()
    _note_source_lag('Omaha', newest)

    candidates = []
    skipped = 0
    for f in rows:
        a = f.get('attributes') or {}
        insp_id = a.get('inspection_id')
        insp_date = sane_inspection_date(str(a.get('inspection_date') or '')[:10])
        name = _oma_clean(a.get('est_name'))
        url = (a.get('downloadDetailsUrl') or '').strip()
        state = (a.get('est_state') or '').strip().upper()
        # The layer carries a couple of test permits and one out-of-state
        # itinerant vendor; only Nebraska rows belong on the map.
        if (insp_id is None or not insp_date or not name or not url.startswith('http')
                or state not in ('NE', 'NEBRASKA') or 'test permit' in name.lower()):
            skipped += 1
            continue
        geom = f.get('geometry') or {}
        lat, lng = geom.get('y'), geom.get('x')
        if not (isinstance(lat, (int, float)) and isinstance(lng, (int, float))
                and 39.5 < lat < 43.5 and -105 < lng < -95):
            lat = lng = None
        candidates.append((insp_date, int(insp_id), a, url, lat, lng))
    if skipped:
        log.info(f"  Omaha: skipped {skipped} rows without an id/date/name/PDF or outside NE")
    candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)   # newest first
    if limit:
        candidates = candidates[:limit]
    if not candidates:
        log.info("Omaha: 0 inspections")
        return []

    log.info(f"  Omaha: fetching {len(candidates)} inspection PDFs "
             f"({OMAHA_DETAIL_WORKERS} workers)...")
    local = threading.local()

    def get_session():
        if not hasattr(local, 's'):
            local.s = requests.Session()
        return local.s

    def work(cand):
        if _past_deadline():
            return cand, 'deadline'
        info = {}
        pdf = _oma_fetch_pdf(cand[3], get_session(), info)
        time.sleep(OMAHA_DETAIL_DELAY)
        if pdf is None:
            # The county's PDF endpoint crashes on some records every time
            # (measured 2026-10-10: 11 of 472, all HTTP 500 with one of the
            # two Laravel errors below). Only that is permanent; any other
            # failure, a bare 500 included, is transient and held.
            body = info.get('body') or ''
            if info.get('status') == 500 and any(sig in body for sig in OMAHA_PERMANENT_500S):
                return cand, 'server'
            return cand, 'download'
        try:
            return cand, _oma_parse_report(pdf)
        except Exception as e:  # noqa: BLE001 - a bad PDF drops one inspection
            log.debug(f"  Omaha PDF {cand[3]}: unparseable: {e}")
            return cand, 'parse'

    reports = {}
    failures = defaultdict(int)
    done = 0
    started = time.time()
    # Oldest first, so a deadline cut drops the newest tail; transient
    # drops hold Omaha's freshness so the next run's lookback re-reads them.
    with ThreadPoolExecutor(max_workers=OMAHA_DETAIL_WORKERS) as pool:
        ordered = sorted(candidates, key=lambda t: (t[0], t[1]))
        for future in as_completed([pool.submit(work, c) for c in ordered]):
            cand, report = future.result()
            done += 1
            if isinstance(report, dict):
                reports[cand[1]] = report
            else:
                failures[report] += 1
                if report in ('download', 'deadline'):
                    _hold_freshness(OMAHA_SOURCE, cand[0], counted=(report == 'download'),
                                    key=cand[1])
            if done % 200 == 0:
                log.info(f"  Omaha PDFs: {done}/{len(candidates)}")
    log.info(f"  Omaha: {len(reports)} PDFs read in {time.time() - started:.0f}s")
    if failures.get('deadline'):
        _set_group_status('Omaha', 'skipped',
                          f"fetch deadline reached; {failures['deadline']} PDFs not read")
    if failures:
        # Dropped, not shipped clean: transient drops ('download',
        # 'deadline') hold the freshness so the lookback refetches them;
        # 'server' (the county's endpoint 500s on that record every time)
        # and 'parse' are permanent and retried only while in the window.
        log.warning("  Omaha: dropped " + ', '.join(
            f"{n} ({why})" for why, n in sorted(failures.items())) +
            " inspections whose PDF could not be read")

    results = []
    date_mismatch = 0
    for insp_date, insp_id, a, url, lat, lng in candidates:
        report = reports.get(insp_id)
        if report is None:
            continue
        p, pf, c = report['totals']
        sev = _oma_item_severities(report['out_items'], report['totals'])
        violations = []
        for num, title in report['out_items']:
            cat, _, _ = classify_violation(title)
            violations.append([cat, sev[num], title[:500]])
        rating = (a.get('rating') or '').strip().upper()
        if rating not in ('A', 'B', 'C', 'D', 'F'):
            rating = (report.get('rating') or '').strip().upper()
            if rating not in ('A', 'B', 'C', 'D', 'F'):
                rating = ''                 # junk values ('-', 'aA', 'NA') and nulls
        pdf_date = report.get('date') or ''
        try:
            if pdf_date and datetime.strptime(pdf_date, '%m/%d/%Y').strftime('%Y-%m-%d') != insp_date:
                date_mismatch += 1
        except ValueError:
            pass
        address = _oma_clean(a.get('est_address'))
        address_2 = _oma_clean(a.get('address_2'))
        if address_2:
            address = f'{address} {address_2}'.strip()
        # the purpose box prints e.g. 'Routine', 'Follow_up', 'Other ,', 'Other , Fire'
        purpose = ' '.join((report.get('purpose') or '').replace('Follow_up', 'Follow-up')
                           .replace('_', ' ').split()).replace(' ,', ',').strip(' ,;:-')
        results.append({
            'name': _oma_clean(a.get('est_name')),
            'address': address,
            'city': _oma_clean(a.get('est_city')) or 'Omaha',
            'state': 'NE',
            'zip': str(a.get('est_zip') or '')[:5],
            'latitude': lat,
            'longitude': lng,
            'inspection_date': insp_date,
            'original_score': None,         # the county publishes a letter, not a number
            'risk_score': max(0, 100 - 5 * p - 2 * pf - c),
            'priority_violations': p,
            'priority_foundation_violations': pf,
            'core_violations': c,
            'total_violations': p + pf + c,
            'violations': violations,
            'inspection_type': purpose,
            'results': f'Rating {rating}' if rating else '',
            'source': OMAHA_SOURCE,
            'source_url': url,
            'source_id': f'oma_{insp_id}',
            'metro': '',
        })
    if date_mismatch:
        log.warning(f"  Omaha: {date_mismatch} PDFs print a different inspection "
                    f"date than the layer row")
    # Same restaurant inspected twice on one day (a routine visit and its
    # follow-up): D1 keys inspections by restaurant+date and keeps the FIRST
    # record it sees, so put the rated, violation-bearing visit first.
    results.sort(key=lambda r: (r['inspection_date'], bool(r['results']),
                                r['total_violations'], r['source_id']), reverse=True)
    log.info(f"Omaha / Douglas County: {len(results)} inspections")
    return results


# ─── FAIRFAX COUNTY, VA (county ArcGIS; not covered by VDH statewide) ────────
# Rolling 2-year window of pass/fail results (no score, no violation text —
# detail is behind Accela). Result categories map onto representative risk.

FAIRFAX_BASE = ('https://www.fairfaxcounty.gov/gispub2/rest/services/Health/'
                'PermittedFoodEstablishments/FeatureServer')
FAIRFAX_RESULT_RISK = {
    'PASSED': 95, 'PARTIAL PASS': 80,
    'FAILED': 55, 'FAILED - SELF CLOSED': 45,
}


def fetch_fairfax(since_date=None, limit=None):
    log.info(f"Fetching Fairfax County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else '2024-01-01'

    fac_rows = _arcgis_query_all(
        f'{FAIRFAX_BASE}/0',
        out_fields='RECORDID,ESTABLISHMENT_NAME,PERMIT_ADDRESS,PERMIT_CITY,'
                   'PERMIT_STATE,PERMIT_ZIP,ESTABLISHMENT_STATUS')
    facilities = {}
    for f in fac_rows:
        a = f.get('attributes', {})
        geom = f.get('geometry') or {}
        if a.get('RECORDID'):
            a['_lng'], a['_lat'] = geom.get('x'), geom.get('y')
            facilities[str(a['RECORDID'])] = a
    log.info(f"  Fairfax: {len(facilities)} establishments")

    insp_rows = _arcgis_query_all(
        f'{FAIRFAX_BASE}/1', where=f"INSPECTIONDATE >= DATE '{since}'",
        out_fields='INSPECTIONID,RECORDID,INSPECTIONDATE,INSPECTIONRESULT')
    log.info(f"  Fairfax: {len(insp_rows)} inspections since {since}")

    results = []
    for f in insp_rows:
        a = f.get('attributes', {})
        fac = facilities.get(str(a.get('RECORDID') or ''))
        ts = a.get('INSPECTIONDATE')
        result = (a.get('INSPECTIONRESULT') or '').strip().upper()
        if not fac or not ts or result not in FAIRFAX_RESULT_RISK:
            continue
        insp_date = sane_inspection_date(
            datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d'))
        name = (fac.get('ESTABLISHMENT_NAME') or '').strip()
        if not insp_date or not name:
            continue
        results.append({
            'name': name.title() if name.isupper() else name,
            'address': (fac.get('PERMIT_ADDRESS') or '').strip().title(),
            'city': ((fac.get('PERMIT_CITY') or 'Fairfax').strip().title()) or 'Fairfax',
            'state': 'VA',
            'zip': str(fac.get('PERMIT_ZIP') or '')[:5],
            'latitude': fac.get('_lat'),
            'longitude': fac.get('_lng'),
            'inspection_date': insp_date,
            'original_score': None,
            'risk_score': FAIRFAX_RESULT_RISK[result],
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': result.title(),
            'source': 'Fairfax County Open Data',
            'source_url': 'https://www.fairfaxcounty.gov/health-humanservices/food',
            'source_id': f"ffx_{a.get('INSPECTIONID')}",
            'metro': '',
        })
        if limit and len(results) >= limit:
            break
    log.info(f"Fairfax County: {len(results)} inspections")
    return results


# ─── LAS VEGAS (Southern Nevada Health District JSON API) ────────────────────

SNHD_API = ('https://www.southernnevadahealthdistrict.org/wp-json/'
            'snhd-eh-restaurants/v1/restaurants')
SNHD_PAGE = 100          # server caps per_page at 100
# The district's site rate-limits by answering 401 Unauthorized once a client
# has made a few hundred requests in a few minutes (GitHub runners saw it
# after ~60 list pages; a sandbox saw it a few minutes into a 4-worker
# detail phase), so the detail phase runs gently and a 401/429 is a signal to
# back off for a minute or two, not a failure to retry three seconds later.
SNHD_DETAIL_WORKERS = int(os.environ.get('SNHD_DETAIL_WORKERS', '2'))
SNHD_DETAIL_DELAY = float(os.environ.get('SNHD_DETAIL_DELAY', '0.5'))
SNHD_LIST_DELAY = float(os.environ.get('SNHD_LIST_DELAY', '0.4'))
SNHD_RATE_LIMIT_WAIT = float(os.environ.get('SNHD_RATE_LIMIT_WAIT', '90'))   # seconds
_SNHD_RATE_LIMIT_LOCK = threading.Lock()
_SNHD_RATE_LIMITED_UNTIL = 0.0
# Breaker: a throttle or failure that does not lift is a block. Once one
# unbroken throttling episode (no successful request in between) has paused
# SNHD_MAX_TOTAL_PAUSE, or SNHD_BREAKER_TRIP requests in a row exhaust their
# retries for any reason (401/429, a 403 block, 5xx, timeouts), every
# further SNHD request fails at once — otherwise a lasting block burns the
# whole fetch deadline in backoff pauses and starves every later group.
# A 400/404/410 is that one permit's problem: not retried, not counted.
SNHD_MAX_TOTAL_PAUSE = float(os.environ.get('SNHD_MAX_TOTAL_PAUSE', '900'))   # seconds
SNHD_BREAKER_TRIP = 3
# A permit read this run also ships its previous inspections up to this many
# days before the window start (see fetch_vegas): a permit left unread last
# run brings along the earlier inspection that run never read (a downgrade
# before a re-inspection), while the window itself starts at the oldest
# unread permit so a cut run always moves forward by what it read.
# Re-shipping a known date is harmless (both merges key on restaurant+date).
SNHD_PREVIOUS_MARGIN_DAYS = 14
_SNHD_BLOCKED = False
_SNHD_PAUSED_TOTAL = 0.0
_SNHD_EXHAUSTED_STREAK = 0


_SNHD_FAILURES_LOGGED = 0


_SNHD_LAST_ERROR = ''


def _snhd_pause_all(seconds):
    """Hold every SNHD worker back for `seconds` (one shared clock); trips
    the breaker once the run's pauses add up to SNHD_MAX_TOTAL_PAUSE."""
    global _SNHD_RATE_LIMITED_UNTIL, _SNHD_PAUSED_TOTAL, _SNHD_BLOCKED
    with _SNHD_RATE_LIMIT_LOCK:
        now = time.time()
        new_until = max(_SNHD_RATE_LIMITED_UNTIL, now + seconds)
        # Count only the extension, so overlapping pauses from several
        # workers are not added up several times.
        _SNHD_PAUSED_TOTAL += max(0.0, new_until - max(_SNHD_RATE_LIMITED_UNTIL, now))
        _SNHD_RATE_LIMITED_UNTIL = new_until
        if _SNHD_PAUSED_TOTAL >= SNHD_MAX_TOTAL_PAUSE and not _SNHD_BLOCKED:
            _SNHD_BLOCKED = True
            log.error(f"  SNHD: throttled for {_SNHD_PAUSED_TOTAL / 60:.0f} min this run — "
                      f"treating it as a block; remaining SNHD requests are skipped")


def _snhd_wait_if_limited():
    while True:
        remaining = _SNHD_RATE_LIMITED_UNTIL - time.time()
        if remaining <= 0 or _past_deadline():
            return
        time.sleep(min(remaining, 5))


def _snhd_get(url, params=None, retries=4, info=None):
    """GET an SNHD API url; the parsed JSON, or None on failure. With
    `info` (a dict), info['permanent'] says whether the failure was the
    request's own (400/404/410: that permit will never read) rather than
    the district's."""
    global _SNHD_FAILURES_LOGGED, _SNHD_LAST_ERROR, _SNHD_BLOCKED, _SNHD_EXHAUSTED_STREAK
    global _SNHD_PAUSED_TOTAL
    if _SNHD_BLOCKED:
        _SNHD_LAST_ERROR = 'SNHD breaker tripped this run'
        return None
    last_err = None
    permanent = False
    for attempt in range(retries):
        _snhd_wait_if_limited()
        if _SNHD_BLOCKED:
            break
        try:
            r_url, r_params, r_extra = _relay(url, params)
            r = requests.get(r_url, params=r_params,
                             headers={'User-Agent': CHROME_UA, **r_extra}, timeout=60,
                             proxies=_portal_proxies())
            r.raise_for_status()
            data = r.json()
            with _SNHD_RATE_LIMIT_LOCK:
                _SNHD_EXHAUSTED_STREAK = 0
                _SNHD_PAUSED_TOTAL = 0.0      # the throttling episode is over
            return data
        except (requests.RequestException, ValueError) as e:
            last_err = e
            resp = getattr(e, 'response', None)
            status = resp.status_code if resp is not None else None
            if status in (400, 404, 410):
                permanent = True
                if info is not None:
                    info['permanent'] = True
                break
            if attempt == retries - 1:
                break
            if status in (401, 429):
                # The district is throttling this client. Every worker
                # pauses together (a lone sleeping worker changes nothing
                # while three others keep hammering), and a 429's
                # Retry-After is honoured when it is longer.
                wait = SNHD_RATE_LIMIT_WAIT * (attempt + 1)
                try:
                    wait = max(wait, min(int((resp.headers or {}).get('Retry-After', 0)), 300))
                except (TypeError, ValueError):
                    pass
                if _past_deadline():
                    break
                if _SNHD_FAILURES_LOGGED < 5:
                    log.warning(f"  SNHD rate-limited ({status}); pausing all SNHD "
                                f"requests {wait:.0f}s (attempt {attempt + 1}/{retries})")
                _snhd_pause_all(wait)
                continue
            # Other failures: back off harder than a couple of seconds.
            if _past_deadline():
                break
            time.sleep(5 * (3 ** attempt))
    # Say WHY: a 403 from an IP the district's site blocks looks nothing
    # like a timeout, and "list fetch failed" alone hid that for weeks.
    # Only the first few failures are logged so a banned detail phase
    # doesn't emit thousands of lines.
    _SNHD_LAST_ERROR = str(last_err)[:120] if last_err else 'SNHD breaker tripped this run'
    if last_err is not None and not permanent:
        with _SNHD_RATE_LIMIT_LOCK:
            _SNHD_EXHAUSTED_STREAK += 1
            if _SNHD_EXHAUSTED_STREAK >= SNHD_BREAKER_TRIP and not _SNHD_BLOCKED:
                _SNHD_BLOCKED = True
                log.error(f"  SNHD: {_SNHD_EXHAUSTED_STREAK} requests in a row failed through "
                          f"every retry ({_SNHD_LAST_ERROR}) — treating it as a block")
    if _SNHD_FAILURES_LOGGED < 5:
        _SNHD_FAILURES_LOGGED += 1
        log.warning(f"  SNHD request failed after {retries} attempts "
                    f"({url.replace(SNHD_API, '<api>')}): {last_err}")
    return None


def _snhd_violations(resolved, demerits_by_sev=True):
    """Convert SNHD resolved-violation dicts to (cat, sev, desc) tuples."""
    out = []
    for v in resolved or []:
        desc = (v.get('description') or '').strip()
        if not desc:
            continue
        try:
            dem = int(v.get('demerits') or 0)
        except (TypeError, ValueError):
            dem = 0
        # SNHD assesses 5 demerits for critical items, 3 for major, 0 for
        # observations/good-practice notes.
        sev = 'priority' if dem >= 5 else ('priority_foundation' if dem >= 3 else 'core')
        cat, _, _ = classify_violation(desc)
        out.append([cat, sev, desc[:500]])
    return out


def fetch_vegas(since_date=None, limit=None, fetch_violations=True):
    """
    Fetch Las Vegas area inspections from the Southern Nevada Health
    District's live JSON API (~18k permitted food establishments across
    Las Vegas, Henderson, North Las Vegas, and the rest of Clark County).
    The list endpoint carries each permit's current OFFICIAL grade,
    demerits, coordinates, and inspection date; the per-permit detail adds
    violation descriptions with demerit values and prior inspections.
    SNHD demerit bands map exactly onto risk = 100 - demerits
    (A: 0-10 -> 90+, B: 11-20 -> 80-89, C: 21-40 -> 60-79).
    """
    log.info(f"Fetching Las Vegas (SNHD) data (since={since_date}, limit={limit})")
    sd = str(since_date)[:10] if since_date else None
    # While a backlog drains over several cut runs, a permit left unread can
    # be re-inspected and re-sorted to the newest end, and be read only once
    # the window is weeks past the inspection it skipped. 'backlog_from' (the
    # first window start of an unbroken series of runs that left permits
    # unread) lets its previous inspections reach back that far.
    fe = ((_FRESHNESS['sources'].setdefault('Southern Nevada Health District', {'group': 'Las Vegas'}))
          if _FRESHNESS is not None else {})
    base = min(sd, fe['backlog_from']) if (sd and fe.get('backlog_from')) else sd
    prev_floor = ((datetime.strptime(base, '%Y-%m-%d') - timedelta(days=SNHD_PREVIOUS_MARGIN_DAYS))
                  .strftime('%Y-%m-%d') if base else None)

    permits, page = {}, 1
    while True:
        d = _snhd_get(SNHD_API, {'per_page': SNHD_PAGE, 'page': page})
        if d is None:
            # A 401/403 here is the district's site refusing this egress
            # (GitHub runners get it after a few dozen pages); a timeout is
            # not. Put the reason in the run report, not just the log.
            raise RuntimeError(f'SNHD list fetch failed on page {page}: {_SNHD_LAST_ERROR}')
        rows = d.get('results') or []
        for row in rows:
            pn = row.get('permit_number')
            if pn:
                permits[pn] = row
        if len(rows) < SNHD_PAGE:
            break
        page += 1
        if page % 30 == 0:
            log.info(f"  SNHD list: {len(permits)} permits so far...")
        time.sleep(SNHD_LIST_DELAY)
    log.info(f"  SNHD: {len(permits)} permits listed")

    # Only permits inspected in the window need (re)fetching.
    todo = []
    for pn, row in permits.items():
        d_cur = str(row.get('date_current') or '')[:10]
        if not d_cur:
            continue
        if sd and d_cur < sd:
            continue
        todo.append(pn)
    # Oldest first, so a breaker trip or a deadline cut leaves the newest
    # permits unread; their hold makes the next run read them.
    todo.sort(key=lambda pn: str(permits[pn].get('date_current') or ''))
    if limit:
        todo = todo[:limit]

    def build(row, insp_date, grade, demerits, insp_type, violations):
        try:
            dem = max(0, int(demerits))
        except (TypeError, ValueError):
            dem = 0
        name = (row.get('restaurant_name') or row.get('location_name') or '').strip()
        return {
            'name': name.title() if name.isupper() else name,
            'address': (row.get('address') or '').strip().title(),
            'city': (row.get('city_name') or 'Las Vegas').strip(),
            'state': 'NV',
            'zip': (row.get('zip_code') or '')[:5],
            'latitude': row.get('latitude'),
            'longitude': row.get('longitude'),
            'inspection_date': insp_date,
            'original_score': dem,
            'risk_score': max(0, 100 - dem),
            'priority_violations': sum(1 for v in violations if v[1] == 'priority'),
            'priority_foundation_violations': sum(1 for v in violations if v[1] == 'priority_foundation'),
            'core_violations': sum(1 for v in violations if v[1] == 'core'),
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': (insp_type or '').strip(),
            'results': f'"{grade}" Grade' if grade else '',
            'source': 'Southern Nevada Health District',
            'source_url': 'https://www.southernnevadahealthdistrict.org/permits-and-regulations/restaurant-inspections/restaurant-inspection-search/',
            'source_id': f"snhd_{row.get('permit_number')}_{insp_date}",
            'metro': 'Las Vegas',
        }

    results = []
    skipped = 0
    if fetch_violations:
        log.info(f"  SNHD: fetching details for {len(todo)} permits "
                 f"({SNHD_DETAIL_WORKERS} workers)...")

        def work(pn):
            if _past_deadline() or _SNHD_BLOCKED:
                return pn, None, 'cut'
            info = {}
            d = _snhd_get(f'{SNHD_API}/{pn}', info=info)
            time.sleep(SNHD_DETAIL_DELAY)
            if d is not None:
                return pn, d, ''
            if info.get('permanent'):
                return pn, None, 'permanent'
            # A breaker trip or the deadline during this request is the
            # run's problem, not the permit's.
            return pn, None, 'cut' if (_SNHD_BLOCKED or _past_deadline()) else 'failed'

        unread = {'failed': [], 'cut': []}
        permanent = 0
        done = 0
        with ThreadPoolExecutor(max_workers=SNHD_DETAIL_WORKERS) as pool:
            for future in as_completed([pool.submit(work, pn) for pn in todo]):
                pn, d, why = future.result()
                done += 1
                if done % 1000 == 0:
                    log.info(f"  SNHD details: {done}/{len(todo)}")
                if d is None:
                    # Unreadable permit: skip rather than record without
                    # violation context (same policy as the Tyler portals).
                    # A 400/404/410 is that permit's own and never reads;
                    # anything else holds the freshness (below).
                    skipped += 1
                    if why == 'permanent':
                        permanent += 1
                    else:
                        unread[why].append(pn)
                    continue
                row = permits[pn]
                cur_date = sane_inspection_date(str(d.get('date_current') or row.get('date_current') or '')[:10])
                if cur_date:
                    results.append(build(
                        row, cur_date, d.get('current_grade') or row.get('current_grade'),
                        d.get('current_demerits') if d.get('current_demerits') is not None else row.get('current_demerits'),
                        d.get('inspection_type') or row.get('inspection_type'),
                        _snhd_violations(d.get('current_violations_resolved'))))
                for prev in d.get('previous_inspections') or []:
                    p_date = sane_inspection_date(str(prev.get('inspection_date') or '')[:10])
                    if not p_date or (prev_floor and p_date < prev_floor):
                        continue
                    results.append(build(
                        row, p_date, prev.get('inspection_grade'),
                        prev.get('inspection_demerits'),
                        prev.get('inspection_type'),
                        _snhd_violations(prev.get('violations_resolved'))))
    else:
        for pn in todo:
            row = permits[pn]
            cur_date = sane_inspection_date(str(row.get('date_current') or '')[:10])
            if cur_date:
                results.append(build(row, cur_date, row.get('current_grade'),
                                     row.get('current_demerits'),
                                     row.get('inspection_type'), []))
    # Hold Las Vegas so the next run re-reads the unread permits: each permit
    # holds on its own (its failures count on their own), and the next
    # window starts exactly at the oldest unread permit's current date, so a
    # run cut short by the breaker or the deadline still moves the window
    # forward by what it read (permits run oldest-first). The permit's
    # earlier inspections come back through SNHD_PREVIOUS_MARGIN_DAYS.
    # _hold_window_start, not a plain hold: the lookback reaches two days
    # past a plain hold, so the window would slide back every run. advance:
    # every permit dated before the oldest unread one was read (oldest-first,
    # all workers done), so the latest may rise to it; otherwise the next
    # window would start a day early and a run that cannot finish two days
    # of permits would never move.
    for why, pns in unread.items():
        for pn in pns:
            cur = sane_inspection_date(str(permits[pn].get('date_current') or '')[:10])
            if cur:
                _hold_window_start('Southern Nevada Health District', cur,
                                   counted=(why == 'failed'), key=pn, advance=True)
    if fetch_violations and sd:
        if any(unread.values()):
            fe.setdefault('backlog_from', sd)
        else:
            fe.pop('backlog_from', None)
    if skipped:
        log.warning(f"  SNHD: skipped {skipped} permits with unreadable details"
                    + (f" ({permanent} answered 400/404/410 and are not retried)" if permanent else ''))
        if _SNHD_BLOCKED:
            _set_group_status('Las Vegas', 'blocked',
                              f'SNHD throttled the detail phase; {skipped} permits not read')
        elif _past_deadline():
            _set_group_status('Las Vegas', 'skipped',
                              f'fetch deadline reached; {skipped} permits not read')
    log.info(f"Las Vegas (SNHD): {len(results)} inspections")
    return results


# ─── MYHEALTHDEPARTMENT PORTAL SCRAPER (DFW + generic) ─────────────────────
# Uses direct HTTP POST to the portal's JSON API (no browser needed).
# The API is the same one the portal's JavaScript frontend calls.

MHD_BASE_URL = 'https://inspections.myhealthdepartment.com/'
MHD_PAGE_SIZE = 25     # server-side hard cap; larger 'count' values are ignored
MHD_QUERY_CAP = 225    # search API silently truncates a query around 225 records
MHD_DELAY = float(os.environ.get('MHD_DELAY', '0.8'))  # seconds between search API calls
MHD_RETRY_DELAY = 30   # seconds on 403/429 before retry
MHD_MAX_RETRIES = 3

# The portal WAF sometimes hard-blocks an IP for a whole run — GitHub
# Actions runners especially, where the block has been observed to persist
# for weeks. Once one request has exhausted its 403/429 retries, every
# further MHD call would burn the same retry sleeps and fail the same way
# (~15 jurisdictions × minutes of sleep = a timed-out CI job), so the first
# exhaustion trips this breaker and the portal's remaining fetches are
# skipped immediately. Skipped jurisdictions yield no records — never
# "clean" — and the data.js/D1 merges preserve their existing data.
# Search exhaustion trips it directly; detail pages 403 transiently under
# rate limiting too, so they must exhaust on several CONSECUTIVE pages
# (a streak, reset by any success) before it reads as a portal-wide block.
_MHD_PORTAL_BLOCKED = False
_MHD_BLOCK_LOCK = threading.Lock()
_MHD_DETAIL_BLOCK_STREAK = 0
MHD_DETAIL_BLOCK_TRIP = 3
# A detail-page ban is temporary: during the Aug 2026 backfills the portal
# lifted it ~15 minutes after the requests stopped, and each window allowed
# a few hundred pages. So instead of abandoning the jurisdiction (and every
# one after it) the bulk fetcher waits the ban out and resumes — at most
# MHD_BAN_COOLDOWNS times per run, and only while the fetch deadline leaves
# room for the wait. Searches keep working during a detail ban.
MHD_BAN_COOLDOWN = float(os.environ.get('MHD_BAN_COOLDOWN', '900'))   # seconds
MHD_BAN_COOLDOWNS = int(os.environ.get('MHD_BAN_COOLDOWNS', '16'))    # per run
# (16 x 15 min matches the 240-minute fetch deadline the weekly job runs
# with, so in CI the deadline, not this count, is what ends a run; the count
# matters for local catch-ups with a longer deadline. A week of every MHD
# jurisdiction is well over 1,000 detail pages, five or more ban windows —
# the old budget of 3 could never finish a normal week once the portal was
# reachable again.)
_MHD_DETAIL_BANNED = False   # detail pages 403ing right now (searches still fine)
_MHD_COOLDOWNS_USED = 0
# Search window length. ONE day: any multi-day window answers one row per
# permit (its latest inspection in the window), so a 7-day window lost the
# earlier inspections of every permit re-inspected inside it — measured
# 2026-11-03 (Adams County: three inspections of one permit on consecutive
# days, one row back from every 7-day read). A multi-day window set by a
# config is still bisected when it reaches the query cap.
MHD_WINDOW_DAYS = 1
# The search API allows roughly 250 requests per TCP CONNECTION before it
# answers 403 to everything on that connection (measured 2026-10-10: 252
# searches in 265 s, then 403; clearing cookies changed nothing, closing the
# session — a fresh connection — got 200 at once). The 7-day DFW windows
# never reached it; Tennessee's bucket mode (30 searches a day) hit it in
# five minutes and the run-wide breaker then wrote the whole state off as
# "blocked". So every session rotates its connections proactively, and the
# first 403 on a search is answered with a reconnect, not a 30 s wait.
MHD_SEARCHES_PER_CONNECTION = int(os.environ.get('MHD_SEARCHES_PER_CONNECTION', '150'))
# Detail-page pace, env-overridable: the portal rate-bans IPs that hammer
# detail pages (searches survive while details start 403ing). When a run
# trips the block, retry later with MHD_DETAIL_WORKERS=1 and a larger
# MHD_DETAIL_DELAY to stay under the radar.
MHD_DETAIL_WORKERS = int(os.environ.get('MHD_DETAIL_WORKERS', '3'))
MHD_DETAIL_DELAY = float(os.environ.get('MHD_DETAIL_DELAY', '0.4'))
# (restaurant_id, 'YYYY-MM-DD') pairs already stored with verified violation
# details. Detail-page requests are the scarce resource — the portal bans
# after a modest number per run — so a backfill resumed across several runs
# must not spend them re-fetching inspections it already has. Loaded from
# --skip-known-inspections; the merge keeps the skipped records' existing data.
SKIP_KNOWN_INSPECTIONS = set()
# display name -> the newest inspection date among the rows a jurisdiction's
# searches returned this run, recorded only by a COMPLETE read whose window
# reached today (not cut by the deadline, a failed window or the record
# limit). _mhd_group_lag_note turns the group's newest into the freshness
# report's source-lag note, so a STALE flag on a group whose portal is
# behind (VDH ran a week late on 2026-11-02) reads as the source's lag, not
# a broken fetch. The self-healing lookback makes this work: a quiet source
# is re-read from its last shipped day, so the rows it still holds are seen.
_MHD_NEWEST_SEEN = {}
# display name -> (unreadable, tried) for a jurisdiction whose detail phase
# failed on most of its pages this run for a reason other than a ban or the
# deadline (a 404 after the detail path moved, a template the parser does
# not know, the welcome page a renamed district slug serves). The records
# are dropped and held either way; this is what makes fetch_dfw REPORT the
# jurisdiction, so the group goes red instead of 'ok' on the other
# jurisdictions' records while the held records are given up after
# FRESHNESS_HOLD_GIVE_UP_RUNS quiet runs.
_MHD_DETAIL_FAILED = {}
# A detail phase's failures are systemic, not transient, when every page
# tried failed, or more than half of at least this many did. One flaky page
# among many is held and re-read next run, not a red run (the same rule as
# OC_PDF_MASS_FAILURE_MIN for Orange County's printables).
MHD_DETAIL_MASS_FAILURE_MIN = 20


def _mhd_detail_mass_failure(unreadable, tried):
    """True when `unreadable` of `tried` detail pages failing looks like the
    jurisdiction's detail pages cannot be read at all (see _MHD_DETAIL_FAILED)."""
    if not tried or not unreadable:
        return False
    return unreadable >= tried or (tried >= MHD_DETAIL_MASS_FAILURE_MIN
                                   and unreadable * 2 > tried)


def _mhd_session():
    """Create a requests session with browser-like headers for MHD portal."""
    s = _PortalSession()
    s.headers.update({
        'User-Agent': CHROME_UA,
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://inspections.myhealthdepartment.com',
        'Referer': 'https://inspections.myhealthdepartment.com/',
    })
    if PORTAL_PROXY_URL:
        s.proxies.update(_portal_proxies())
    return s


def _mhd_path(slug, config=None):
    """The portal path (URL slug) behind a jurisdiction-table key.

    Keys are fetch units: usually the portal slug itself, but a config that
    carries 'path' is a pseudo-jurisdiction — one of several units carved
    out of one portal slug (the Tennessee metros all live under
    'tennessee'). Every request URL and payload must use this, never the
    key."""
    return str((config or {}).get('path') or slug)


def _mhd_norm_city(value):
    """City string as compared against a config's city_allow: lower-case,
    whitespace collapsed, hyphens as spaces, periods dropped ('Soddy-Daisy'
    == 'Soddy Daisy', 'Mt. Juliet' == 'Mt Juliet', 'NASHVILLE' == 'Nashville')."""
    return ' '.join(str(value or '').lower().replace('-', ' ').replace('.', '').split())


def _mhd_row_rejection(rec, programs=(), city_allow=(), exclude=()):
    """Why a raw search row falls outside a jurisdiction config ('' = keep):
    'program' when the config lists programs and the row's programName is not
    one of them, 'city' when it lists city_allow and the row's city is not
    one of them, 'excluded' when one of the config's exclude patterns
    ((row field, compiled regex) pairs from _mhd_exclude_patterns) matches
    the row's field. The sets arrive pre-normalised (lower-case programs,
    _mhd_norm_city cities); an empty set means "no filter"."""
    if programs and str(rec.get('programName') or '').strip().lower() not in programs:
        return 'program'
    if city_allow and _mhd_norm_city(rec.get('city')) not in city_allow:
        return 'city'
    for field, pattern in exclude:
        if pattern.search(str(rec.get(field) or '')):
            return 'excluded'
    return ''


def _mhd_exclude_patterns(config):
    """Compiled (row field, regex) pairs from a config's 'exclude' mapping
    ({field: regex}); a row whose field matches is dropped before parsing.
    Arapahoe County's mobile units are the case: addressLine1 '^Mobile Unit'
    with no city and no zip — no fixed address to geocode or to map."""
    return [(str(field), re.compile(str(rx))) for field, rx in
            (config.get('exclude') or {}).items()]


# Sorting the plain date search. The portal pages a query by `start` offset,
# and with 'sort': {} (its default) the order is UNSTABLE on inspectionDate
# ties: page N+1 re-serves rows page N already had and never serves others.
# Measured 2026-11-03 on the jurisdictions DineScores ships, over two
# completed weeks each: the weekly unsorted 7-day read lost 3-13% of the
# rows a day-by-day read found (Dallas 20 of 154, Frisco 5 of 56, El Paso
# County 9 of 77, Adams County 5 of 59), while sorting on the unique
# inspectionID served disjoint, complete pages on every read. Every lossy
# unsorted read showed its loss as rows REPEATED across pages (as many
# repeats as rows lost), so _mhd_pages_consistent refuses a sorted page
# sequence with a repeat rather than shipping it short. It does NOT check
# the order of the keys: most portals serve the sort in SQL Server
# uniqueidentifier order, but Gwinnett's pages — disjoint and complete,
# first page 45-60 rows long — are not in that order, so an order check
# would fail a correct read. Orange County's newest-225 poll must NOT sort
# (any sort turns its query into a whole-database one, see
# fetch_orange_county), so the default lives in the jurisdiction fetch, not
# in _mhd_search_page.
MHD_SORT_INSPECTION_ID = {'field': 'inspectionID', 'direction': 'asc'}
MHD_DEFAULT_SORT = MHD_SORT_INSPECTION_ID
# A sorted query whose pages do not fit together is read again once (a row
# posted between two page requests shifts the sequence by one) before the
# window is failed.
MHD_SORTED_READ_ATTEMPTS = 2


def _mhd_pages_consistent(pages, sort):
    """'' when a sorted query's pages fit together as one stable sequence,
    else 'repeat': an inspectionID appeared on two pages, which means the
    portal did not page the query stably (the unstable default order
    re-serves rows from an earlier page in place of rows it never serves),
    so the rows are not a complete read of the window. `sort` is the
    payload sort the pages were read with (kept for the log and for a
    field-specific check, should one ever prove universal)."""
    seen_on = {}
    for page_no, page in enumerate(pages):
        for row in page:
            iid = str(row.get('inspectionID') or '').strip()
            if not iid:
                continue
            if seen_on.get(iid, page_no) != page_no:
                return 'repeat'
            seen_on[iid] = page_no
    return ''


def _mhd_rotate_connection(session, force=False):
    """Count this session's searches and drop its pooled connections every
    MHD_SEARCHES_PER_CONNECTION of them (or now, when force), so the next
    request opens a fresh connection with a fresh quota. Cookies are kept:
    they are not what the quota is keyed on."""
    n = getattr(session, '_mhd_searches', 0)
    n = (n if isinstance(n, int) else 0) + 1
    if force or n >= MHD_SEARCHES_PER_CONNECTION:
        session.close()   # requests reopens connections on the next request
        n = 0
    session._mhd_searches = n


def _mhd_search_page(session, path, date_range_str, start_offset, retry_count=0,
                     search_str='', purpose='', sort=None):
    """
    Fetch one page of search results from MHD portal API.

    Args:
        session: requests.Session with browser headers
        path: portal path (e.g. 'dallas'; see _mhd_path)
        date_range_str: date filter string like '2026-03-01 to 2026-03-01'
        start_offset: pagination offset
        retry_count: current retry attempt
        search_str: portal searchStr — '' (default) is the plain date search;
            non-empty switches the portal to one row per permit (see the
            Tennessee note above MHD_METRO_JURISDICTIONS)
        purpose: filters.purpose — '' (default) is any purpose
        sort: the payload's sort ({'field': ..., 'direction': 'asc'|'desc'});
            None (default) sends the portal's own unstable order (see
            MHD_DEFAULT_SORT) — Orange County's poll depends on that

    Returns:
        list of record dicts, or None on permanent failure
    """
    payload = {
        'data': {
            'path': path,
            'programName': '',   # never non-empty: the server hangs on it
            'filters': {
                'date': date_range_str,
                'purpose': purpose or '',
            },
            'start': start_offset,
            'count': MHD_PAGE_SIZE,
            'searchStr': search_str or '',
            'lat': 0,
            'lng': 0,
            'sort': dict(sort) if sort else {},
        },
        'task': 'searchInspections',
    }
    global _MHD_PORTAL_BLOCKED
    if _MHD_PORTAL_BLOCKED:
        return None
    _mhd_rotate_connection(session)
    try:
        r = session.post(MHD_BASE_URL, json=payload, timeout=30)
        if r.status_code in (403, 429):
            if retry_count == 0 and getattr(session, '_mhd_reconnected', False) is not True:
                # A per-connection quota, not a block: reconnect and retry
                # right away. The flag keeps one quota exhaustion from
                # masking a real block (the second 403 goes to the ladder).
                _mhd_rotate_connection(session, force=True)
                session._mhd_reconnected = True
                log.info(f"  MHD {r.status_code} on a search — reconnected and retrying")
                result = _mhd_search_page(session, path, date_range_str, start_offset,
                                          retry_count, search_str, purpose, sort)
                session._mhd_reconnected = False
                return result
            if retry_count < MHD_MAX_RETRIES:
                if _past_deadline():
                    # Time budget ran out, not necessarily a persistent
                    # block — give up on this request without tripping the
                    # run-wide breaker (the deadline skips everything anyway).
                    log.warning(f"  MHD blocked ({r.status_code}) — fetch deadline "
                                f"reached, not retrying")
                    return None
                wait = MHD_RETRY_DELAY * (retry_count + 1)
                log.warning(f"  MHD blocked ({r.status_code}). Waiting {wait}s before retry "
                            f"{retry_count + 1}/{MHD_MAX_RETRIES}...")
                time.sleep(wait)
                return _mhd_search_page(session, path, date_range_str, start_offset,
                                        retry_count + 1, search_str, purpose, sort)
            _MHD_PORTAL_BLOCKED = True
            log.error(f"  MHD blocked ({r.status_code}) after {retry_count} retries — "
                      f"skipping the portal's remaining fetches this run")
            return None
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            # The portal answers a request it rejects with 200 and
            # {"err": true, "msg": "bad request"} — a path it does not know
            # (a renamed district slug), an offset past the cap (measured
            # 2026-11-03) — while an empty day is a plain []. So this is not
            # "no inspections today" but a window that was not read: None,
            # which the jurisdiction fetch holds and fails closed on.
            log.error(f"  MHD search {path} {date_range_str} start={start_offset}: the portal "
                      f"answered {type(data).__name__} {str(data)[:100]!r}, not a result "
                      f"list — the window is unread")
            return None
        return data
    except requests.RequestException as e:
        if retry_count < MHD_MAX_RETRIES and not _past_deadline():
            wait = MHD_RETRY_DELAY * (retry_count + 1)
            log.warning(f"  MHD network error: {e}. Waiting {wait}s before retry...")
            time.sleep(wait)
            return _mhd_search_page(session, path, date_range_str, start_offset,
                                    retry_count + 1, search_str, purpose, sort)
        log.error(f"  MHD failed after {retry_count} retries: {e}")
        return None


def _mhd_search_all_pages(session, path, date_range, search_str='', purpose='',
                          sort=None, label=''):
    """
    Page one search query (offsets of MHD_PAGE_SIZE) until a short page or
    the query cap. The first page can carry more than MHD_PAGE_SIZE rows and,
    without a sort, later pages can repeat rows (both seen on 'gwinnett' and
    'tennessee'), so callers dedupe by inspectionID; offsets are never pushed
    past the cap (the portal rejects them).

    With a `sort` the pages must fit together (_mhd_pages_consistent): a row
    repeated across pages means the portal did not page the query stably
    and the window is NOT complete — the query then fails (None, as a 403
    exhaustion does) so the caller re-reads the window instead of shipping
    it short.

    Returns the raw rows (len >= MHD_QUERY_CAP means the cap was hit), or
    None on permanent failure.
    """
    for attempt in range(MHD_SORTED_READ_ATTEMPTS):
        rows = []
        pages = []
        start = 0
        while len(rows) < MHD_QUERY_CAP:
            page = _mhd_search_page(session, path, date_range, start,
                                    search_str=search_str, purpose=purpose, sort=sort)
            if page is None:
                return None  # permanent failure
            if not page:
                break
            rows.extend(page)
            pages.append(page)
            if len(page) < MHD_PAGE_SIZE:
                break
            start += MHD_PAGE_SIZE
            time.sleep(MHD_DELAY)
        if not (sort and len(pages) > 1):
            return rows
        why = _mhd_pages_consistent(pages, sort)
        if not why:
            return rows
        if attempt + 1 < MHD_SORTED_READ_ATTEMPTS and not _past_deadline():
            # A row posted between two page requests shifts the sequence
            # once; a re-read of the whole query settles it. Anything that
            # repeats on the re-read is the portal, not timing.
            log.warning(f"  {label or path} {date_range}: sorted pages are not one stable "
                        f"sequence ({why}: {len(pages)} pages, {len(rows)} rows) — "
                        f"re-reading the query")
            time.sleep(MHD_DELAY)
            continue
        log.error(f"  {label or path} {date_range}: sorted pages are not one stable "
                  f"sequence ({why}: {len(pages)} pages, {len(rows)} rows) — the "
                  f"window cannot be read completely, failing the query")
        return None
    return None


def _mhd_fetch_bucket_window(session, path, start_day, end_day, search_terms,
                             purposes, label='', sort=None):
    """
    Bucket-mode search for one window: one paged query per search term ×
    purpose, unioned by inspectionID (a permit matching two terms, e.g. a
    'Nashville' address in 'Madison', comes back from both).

    A term+purpose that still reaches MHD_QUERY_CAP is logged as incomplete
    and KEPT, never bisected: with a searchStr the window is already a
    single day and the result is one row per permit, so there is nothing
    finer to split — only a narrower term would help.

    Returns (rows, capped) where capped lists the (term, purpose) pairs that
    hit the cap, or None on permanent failure.
    """
    date_range = f"{start_day:%Y-%m-%d} to {end_day:%Y-%m-%d}"
    label = label or path
    by_id = {}
    capped = []
    for term in search_terms:
        for purpose in purposes:
            if _past_deadline():
                log.warning(f"  {label} {date_range}: fetch deadline reached inside the "
                            f"window — stopping after {len(by_id)} rows")
                return list(by_id.values()), capped
            rows = _mhd_search_all_pages(session, path, date_range,
                                         search_str=term, purpose=purpose,
                                         sort=sort, label=label)
            if rows is None:
                return None
            log.debug(f"  {label} {date_range} '{term}' / {purpose or 'any purpose'}: "
                      f"{len(rows)} rows")
            if len(rows) >= MHD_QUERY_CAP:
                log.warning(f"  {label} {date_range} '{term}' / {purpose or 'any purpose'}: "
                            f"hit the {MHD_QUERY_CAP} record cap — this bucket is "
                            f"INCOMPLETE (a narrower search term is needed)")
                capped.append((term, purpose))
            for row in rows:
                key = (str(row.get('inspectionID') or '').strip()
                       or f"{row.get('permitID')}|{row.get('inspectionDate')}")
                by_id.setdefault(key, row)
            time.sleep(MHD_DELAY)
    return list(by_id.values()), capped


def _mhd_fetch_window(session, path, start_day, end_day, sort=None, label=''):
    """
    Fetch all inspection records for a date window (datetime, datetime),
    paginating through results. The search API silently truncates a query
    around MHD_QUERY_CAP records, so if we collect that many the window is
    bisected into halves and re-fetched recursively. `sort` and `label` go
    to _mhd_search_all_pages (the sorted-pages guard and its log line).

    Returns a list of raw records, or None on permanent failure.
    """
    date_range = f"{start_day.strftime('%Y-%m-%d')} to {end_day.strftime('%Y-%m-%d')}"
    all_results = _mhd_search_all_pages(session, path, date_range, sort=sort, label=label)
    if all_results is None:
        return None  # permanent failure

    if len(all_results) >= MHD_QUERY_CAP and start_day.date() < end_day.date():
        # Cap hit on a multi-day window: bisect and refetch both halves.
        # Split on CALENDAR-DAY boundaries — the server treats any
        # timestamp as its whole day, so sub-day midpoints re-query the
        # same day forever (and the retry storm ends in a 403 block).
        mid = start_day + (end_day - start_day) / 2
        mid = datetime(mid.year, mid.month, mid.day)
        if mid < start_day:
            mid = datetime(start_day.year, start_day.month, start_day.day)
        log.info(f"  {label or path} {date_range}: hit query cap, splitting window")
        left = _mhd_fetch_window(session, path, start_day, mid, sort, label)
        right = _mhd_fetch_window(session, path, mid + timedelta(days=1), end_day, sort, label)
        if left is None or right is None:
            return None
        return left + right

    if len(all_results) >= MHD_QUERY_CAP:
        log.warning(f"  {label or path} {date_range} hit the {MHD_QUERY_CAP} record cap on a "
                    f"single day — some records may be missing")
    return all_results


def _mhd_parse_detail_html(html):
    """
    Extract violation observation texts from an MHD inspection detail page.

    The portal renders violations server-side in the page HTML:
      <h3 class="observations-header">Observations &amp; Corrective Actions</h3>
      <p class="observations-text">16: 4-601.11(A) OBSERVED ... <br /><br />39: ...</p>

    Jurisdiction templates vary: some use <h5>, and some (e.g. Plano) ship the
    block inside an HTML comment while rendering line items separately — the
    commented-out block still contains the full observation text.

    Returns a list of observation strings (one per violation).
    """
    m = re.search(
        r'Observations\s*&(?:amp;)?\s*Corrective\s*Actions\s*</h\d>\s*'
        r'<p class="observations-text">(.*?)</p>',
        html, re.S | re.I)
    observations = []
    if m:
        block = m.group(1)
        parts = re.split(r'(?:<br\s*/?>\s*){2,}', block)
        for part in parts:
            text = re.sub(r'<[^>]+>', ' ', part)
            text = html_lib.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 5 and not text.lower().startswith('no observation')\
                    and 'window.seenitemnums' not in text.lower():
                observations.append(text[:500])
    if observations:
        return observations

    # Newer templates (e.g. Frisco) render observations client-side from
    # inline JS: var itemNum = "30"; var comments = `...html...`;
    for item_num, comments in re.findall(
            r'itemNum\s*=\s*"(\d+)"\s*;\s*var\s+comments\s*=\s*`([^`]*)`', html):
        text = re.sub(r'<[^>]+>', ' ', comments)
        text = html_lib.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) > 5:
            observations.append(f"{item_num}: {text}"[:500])
    return observations


# What the parser reads on an inspection detail page: the observations block
# (every template probed 2026-11-03 — Fort Bend, Plano, Frisco, Dallas,
# Tarrant, Gwinnett, Multnomah, El Paso County, Utah County, Arapahoe,
# Cuyahoga, VDH Norfolk — renders it, Plano's inside an HTML comment, and
# a clean inspection renders it EMPTY) or Frisco's itemNum/comments JS. The
# portal's welcome page ('Welcome | Virginia Department of Health | My
# Health Department', served for a district's inspection id under another
# path) and its landing page ('My Health Department', served for a path it
# does not know) carry neither, and nor would a template the parser no
# longer understands: such a 200 is not a read of the inspection, and the
# parser would otherwise read it as a CLEAN one (risk 100 on the no-score
# and demerit jurisdictions, where the grade is nothing but this parse).
_MHD_INSPECTION_PAGE_RE = re.compile(
    r'observations-(?:text|header)|itemNum\s*=\s*"\d+"', re.I)


def _mhd_is_inspection_page(html):
    """True when `html` carries the observations block (or the itemNum JS)
    the detail parser reads — i.e. is an inspection page, clean or not."""
    return bool(html) and bool(_MHD_INSPECTION_PAGE_RE.search(html))


def _mhd_fetch_inspection_detail(session, path, inspection_id, retry_count=0):
    """
    Fetch violation details for a single inspection by scraping its public
    detail page. (The portal has no JSON API for details — the observations
    are rendered server-side into the page HTML, so a plain GET suffices.)
    `path` is the real portal path (_mhd_path), never a pseudo-jurisdiction key.

    Returns a list of violation description strings (empty for a clean
    inspection), or None when the page could not be read — a non-200, a
    block, or a 200 that is not an inspection page (_mhd_is_inspection_page).
    """
    global _MHD_DETAIL_BLOCK_STREAK, _MHD_DETAIL_BANNED
    if _MHD_PORTAL_BLOCKED or _MHD_DETAIL_BANNED:
        return None  # blocked — must not read as "no violations"
    url = f'{MHD_BASE_URL}{path}/inspection/'
    try:
        r = session.get(url, params={'inspectionID': inspection_id}, timeout=30)
        if r.status_code in (403, 429):
            if retry_count < 2:
                if _past_deadline():
                    return None  # out of budget, not evidence of a block
                time.sleep(MHD_RETRY_DELAY)
                return _mhd_fetch_inspection_detail(session, path, inspection_id, retry_count + 1)
            with _MHD_BLOCK_LOCK:
                _MHD_DETAIL_BLOCK_STREAK += 1
                if (_MHD_DETAIL_BLOCK_STREAK >= MHD_DETAIL_BLOCK_TRIP
                        and not _MHD_DETAIL_BANNED):
                    # Detail-only ban: the bulk fetcher decides whether to
                    # wait it out or give up on the portal for this run.
                    _MHD_DETAIL_BANNED = True
                    log.warning(f"  MHD blocked {_MHD_DETAIL_BLOCK_STREAK} consecutive "
                                f"detail pages through retries — detail fetches "
                                f"paused")
            return None  # blocked — must not read as "no violations"
        if r.status_code != 200:
            return None
        with _MHD_BLOCK_LOCK:
            _MHD_DETAIL_BLOCK_STREAK = 0
        r.encoding = 'utf-8'  # pages omit charset; default latin-1 mangles § etc.
        if not _mhd_is_inspection_page(r.text):
            # The welcome/landing page, or a template without the block the
            # parser reads: not a read, never "no violations".
            title = re.search(r'<title>(.*?)</title>', r.text or '', re.S | re.I)
            log.debug(f"  Detail page for {inspection_id} under {path} is not an inspection "
                      f"page ({' '.join((title.group(1) if title else 'no title').split())[:80]}) "
                      f"— not read")
            return None
        return _mhd_parse_detail_html(r.text)
    except Exception as e:
        log.debug(f"  Detail fetch failed for {inspection_id}: {e}")
        return None


def _mhd_fetch_details_bulk(path, records, trust_official_score=True, label=None):
    """
    Fetch violation details for many inspections with a small worker pool.
    Each record with details gets its violations/risk fields updated in place.

    path: the real portal path (_mhd_path) the detail URLs live under.
    label: name for log lines (defaults to the path; pseudo-jurisdictions
    pass their display name so four Tennessee metros don't all log as
    'tennessee').
    trust_official_score: True for 0-100 jurisdictions whose official score
    already IS a risk score; False for demerit jurisdictions, where the risk
    score is recomputed from the scraped violations instead.

    The portal bans detail pages after a few hundred per window. When the
    ban trips mid-jurisdiction the remaining inspections are retried after
    a cooldown (see MHD_BAN_COOLDOWN) rather than dropped, as long as the
    per-run cooldown budget and the fetch deadline allow it.

    Returns (fetched_count, records_still_without_details).
    """
    global _MHD_PORTAL_BLOCKED, _MHD_DETAIL_BANNED, _MHD_DETAIL_BLOCK_STREAK
    global _MHD_COOLDOWNS_USED
    label = label or path
    local = threading.local()

    def get_session():
        if not hasattr(local, 'session'):
            local.session = _mhd_session()
        return local.session

    def fetch_one(rec):
        texts = _mhd_fetch_inspection_detail(get_session(), path, rec['source_id'])
        if not (_MHD_PORTAL_BLOCKED or _MHD_DETAIL_BANNED):
            # no pacing needed for the breaker's instant no-op returns
            time.sleep(MHD_DETAIL_DELAY)
        return rec, texts

    def apply_texts(rec, texts):
        violations = [classify_violation(t) for t in texts]
        computed_score, pv, pfv, cv = calc_risk_score(violations)
        rec['violations'] = [[c, s, d] for c, s, d in violations]
        rec['priority_violations'] = pv
        rec['priority_foundation_violations'] = pfv
        rec['core_violations'] = cv
        rec['total_violations'] = len(violations)
        # Keep the health department's official score when present
        # and trustworthy; otherwise use our computed score.
        if not trust_official_score or rec.get('original_score') is None:
            rec['risk_score'] = computed_score

    def can_wait_out_ban():
        if _MHD_COOLDOWNS_USED >= MHD_BAN_COOLDOWNS:
            return False
        if (_FETCH_DEADLINE is not None
                and time.time() + MHD_BAN_COOLDOWN + 120 > _FETCH_DEADLINE):
            return False
        return True

    fetched = 0
    pending = list(records)
    still_failed = []
    plain_retries = 0
    while pending:
        failed = []
        done = 0
        with ThreadPoolExecutor(max_workers=MHD_DETAIL_WORKERS) as pool:
            futures = [pool.submit(fetch_one, rec) for rec in pending]
            for future in as_completed(futures):
                rec, texts = future.result()
                done += 1
                if texts is None:
                    # Blocked/failed fetch — queue for retry; never record the
                    # inspection as violation-free on the strength of a 403.
                    failed.append(rec)
                else:
                    # An EMPTY list is a successfully-read clean inspection —
                    # apply it so no-score jurisdictions get risk 100, not the
                    # neutral placeholder.
                    apply_texts(rec, texts)
                    fetched += 1
                if done % 100 == 0:
                    log.info(f"  Details: {done}/{len(pending)} inspections this pass "
                             f"({fetched}/{len(records)} with details)")
        if not failed:
            break
        if _MHD_PORTAL_BLOCKED:
            still_failed = failed
            break
        if _MHD_DETAIL_BANNED:
            if can_wait_out_ban():
                _MHD_COOLDOWNS_USED += 1
                log.warning(f"  {label}: detail pages banned after {fetched} fetched — "
                            f"waiting {MHD_BAN_COOLDOWN / 60:.0f} min for the ban to "
                            f"lift (cooldown {_MHD_COOLDOWNS_USED}/{MHD_BAN_COOLDOWNS}); "
                            f"{len(failed)} inspections still to detail")
                time.sleep(MHD_BAN_COOLDOWN)
                with _MHD_BLOCK_LOCK:
                    _MHD_DETAIL_BANNED = False
                    _MHD_DETAIL_BLOCK_STREAK = 0
                pending = failed
                continue
            # The ban outlives this run's budget: every later jurisdiction's
            # detail phase would ban instantly too, so trip the run-wide
            # breaker and drop the undetailed records (never ship as clean).
            _MHD_PORTAL_BLOCKED = True
            log.error(f"  {label}: detail pages banned with no cooldown budget left "
                      f"({_MHD_COOLDOWNS_USED}/{MHD_BAN_COOLDOWNS} used, deadline "
                      f"{'set' if _FETCH_DEADLINE else 'none'}) — skipping the "
                      f"portal's remaining fetches this run")
            still_failed = failed
            break
        # Failures without a ban (timeouts, odd pages): one more pass.
        if plain_retries >= 1 or _past_deadline():
            still_failed = failed
            break
        plain_retries += 1
        log.warning(f"  {label}: retrying {len(failed)} failed detail fetches")
        pending = failed
    return fetched, still_failed


def fetch_dfw(jurisdictions=None, since_date=None, limit_per_jurisdiction=None,
              fetch_violations=True, until_date=None, skipped=None):
    """
    Fetch inspection data for DFW jurisdictions using direct API calls to the
    MyHealthDepartment portal. No browser required — uses the same JSON POST
    endpoint that the portal's JavaScript frontend calls.

    Args:
        jurisdictions: dict of slug->config to scrape (defaults to all DFW_JURISDICTIONS)
        since_date: ISO YYYY-MM-DD string for start date (defaults to Jan 1 of current year)
        limit_per_jurisdiction: max records per jurisdiction (None = no limit)
        fetch_violations: if True (default), also scrape each inspection's public
            detail page for violation observations. The portal renders these
            server-side into the HTML, so a plain GET per inspection suffices
            (~350 pages for a weekly Dallas+Plano refresh).

        skipped: optional list; the display name and reason of every
            jurisdiction this call did not fetch (portal blocked, fetch
            deadline, an exception) or whose detail pages mostly failed to
            read (_MHD_DETAIL_FAILED) are appended to it, so the caller can
            report the group as partial instead of 'ok'/'empty'.

    Returns:
        list of inspection record dicts with 'metro': 'DFW'
    """
    if jurisdictions is None:
        jurisdictions = DFW_JURISDICTIONS
    if skipped is None:
        skipped = []

    session = _mhd_session()
    all_results = []

    # Stalest jurisdiction first. A run that hits its deadline or the ban
    # budget always loses the jurisdictions at the END of the list; in a
    # fixed order the same ones would starve every week, while ordering by
    # the recorded latest shipped inspection makes each run work on whatever
    # is furthest behind, so a backlog drains evenly across runs.
    order = sorted(jurisdictions.items(),
                   key=lambda kv: _source_latest(kv[1]['display_name']))
    if len(order) > 1:
        log.info("MHD order (stalest first): " + ', '.join(
            f"{c['display_name']} ({_source_latest(c['display_name']) or 'never'})"
            for _, c in order))

    for slug, config in order:
        display_name = config['display_name']
        if _MHD_PORTAL_BLOCKED:
            log.warning(f"{display_name}: skipped — MHD portal is blocking this run; "
                        f"existing records are preserved by the merge")
            skipped.append(f"{display_name} (portal blocked)")
            continue
        if _past_deadline():
            log.warning(f"{display_name}: skipped — pipeline fetch deadline reached")
            skipped.append(f"{display_name} (deadline)")
            continue
        log.info(f"--- Fetching {display_name} ({slug}) via API ---")
        # since_date may be one date for every jurisdiction or a per-slug
        # mapping (the weekly run's per-jurisdiction windows).
        j_since = since_date.get(slug) if isinstance(since_date, dict) else since_date
        try:
            results = _fetch_mhd_jurisdiction_api(
                session, slug, config, j_since, limit_per_jurisdiction,
                fetch_violations, until_date=until_date)
            all_results.extend(results)
            log.info(f"{display_name}: {len(results)} records")
            if display_name in _MHD_DEADLINE_CUT:
                skipped.append(f"{display_name} (deadline)")
            elif display_name in _MHD_DETAIL_FAILED:
                n_bad, n_tried = _MHD_DETAIL_FAILED[display_name]
                skipped.append(f"{display_name} (error: {n_bad}/{n_tried} detail pages unreadable)")
        except Exception as e:
            log.error(f"{display_name} fetch failed: {e}")
            import traceback
            log.error(traceback.format_exc())
            skipped.append(f"{display_name} (error: {str(e)[:60]})")
        time.sleep(3)  # polite pause between jurisdictions

    log.info(f"DFW total: {len(all_results)} inspection records across "
             f"{len(jurisdictions)} jurisdictions")
    return all_results


def _mhd_group_lag_note(group, jurisdictions, not_fetched=()):
    """Leave the fetch group's source-lag note (_note_source_lag) once, after
    a complete read: the newest inspection date any of its jurisdictions'
    searches returned this run (_MHD_NEWEST_SEEN). The note only shows when
    that date is older than SOURCE_LAG_NOTE_DAYS, i.e. when the whole group
    is behind because the portal is. No note when a jurisdiction was not
    fetched (the report's detail already says which) or none completed a
    read that reached today."""
    if not_fetched:
        return
    newest = max((_MHD_NEWEST_SEEN.get(c['display_name'], '') for c in jurisdictions.values()),
                 default='')
    if newest:
        _note_source_lag(group, newest)


def _fetch_mhd_jurisdiction_api(session, slug, config, since_date=None, limit=None,
                                 fetch_violations=True, until_date=None):
    """
    Fetch all inspections for one MHD jurisdiction using date windows and the
    searchInspections API. Optionally fetches violation details.

    `slug` is the jurisdiction-table key; every request uses
    _mhd_path(slug, config). Config keys 'window_days', 'programs' and the
    bucket trio 'search_terms'/'purposes'/'city_allow' are documented above
    MHD_METRO_JURISDICTIONS.
    """
    display_name = config['display_name']
    default_city = config['default_city']
    default_state = config.get('state', 'TX')
    path = _mhd_path(slug, config)
    score_scale = config.get('score_scale', '100')
    metro = config.get('metro', 'DFW')
    window_days = max(1, int(config.get('window_days') or MHD_WINDOW_DAYS))
    search_terms = [str(t) for t in (config.get('search_terms') or []) if str(t).strip()]
    purposes = [str(p) for p in (config.get('purposes') or [''])]
    programs = {str(p).strip().lower() for p in (config.get('programs') or [])}
    city_allow = {_mhd_norm_city(c) for c in (config.get('city_allow') or [])}
    exclude = _mhd_exclude_patterns(config)
    bucket_mode = bool(search_terms)
    # The plain date search sorts on inspectionID unless the config says
    # otherwise (MHD_DEFAULT_SORT; 'sort': None opts out). Bucket mode keeps
    # the portal's own order unless the config sets one: a searchStr query
    # answers one row per permit and its sorting is unverified.
    sort = config['sort'] if 'sort' in config else (None if bucket_mode else MHD_DEFAULT_SORT)
    if window_days > 1:
        # ANY multi-day window answers one row per permit — that permit's
        # latest inspection in the window (measured 2026-11-03 on Adams
        # County: a permit inspected on three consecutive days came back
        # once from every 7-day read, sorted or not; the same on Summit
        # County OH, Placer County CA). So a window longer than a day loses
        # a permit's earlier inspections, including the Routine that earned
        # a Follow-Up. MHD_WINDOW_DAYS is 1 for that reason.
        log.warning(f"{display_name}: {window_days}-day windows — a permit inspected "
                    f"twice inside a window keeps only its latest inspection; set "
                    f"window_days 1")

    # Build date range
    if since_date:
        try:
            range_start = datetime.strptime(str(since_date)[:10], '%Y-%m-%d')
        except Exception:
            log.warning(f"{display_name}: Could not parse since_date '{since_date}', defaulting to Jan 1")
            range_start = datetime(datetime.now().year, 1, 1)
    else:
        range_start = datetime(datetime.now().year, 1, 1)
    range_end = datetime.now()
    # Bound the scrape for staged historical backfills (--until-date), the
    # same contract as the Houston/DC/Richardson scrapers.
    if until_date:
        try:
            range_end = min(range_end, datetime.strptime(str(until_date)[:10], '%Y-%m-%d'))
        except Exception:
            log.warning(f"{display_name}: Could not parse until_date '{until_date}', ignoring")

    # Build list of windows (multi-day ones are bisected automatically if
    # they hit the portal's query cap; bucket configs use one-day windows)
    windows = []
    current = range_start
    while current <= range_end:
        window_end = min(current + timedelta(days=window_days - 1), range_end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)

    log.info(f"{display_name}: {len(windows)} windows of up to {window_days} days from "
             f"{range_start.strftime('%Y-%m-%d')} to {range_end.strftime('%Y-%m-%d')}"
             + (f" — bucket mode: {len(search_terms)} search terms x {len(purposes)} "
                f"purposes per window" if bucket_mode else ''))

    all_records = []
    seen_ids = set()
    stopped_early = False
    capped_queries = 0
    dropped_by_program = dropped_by_city = dropped_by_exclude = 0
    newest_seen = ''   # newest inspection date among ALL rows returned

    for win_idx, (win_start, win_end) in enumerate(windows):
        if limit and len(all_records) >= limit:
            break
        if _past_deadline():
            log.warning(f"  {display_name}: fetch deadline reached — stopping "
                        f"before window {win_idx + 1}/{len(windows)}")
            stopped_early = True
            _MHD_DEADLINE_CUT.add(display_name)
            break

        capped = []
        if bucket_mode:
            fetched = _mhd_fetch_bucket_window(session, path, win_start, win_end,
                                               search_terms, purposes, label=display_name,
                                               sort=sort)
            raw_records = None if fetched is None else fetched[0]
            capped = [] if fetched is None else fetched[1]
            if fetched is not None and _past_deadline():
                # The deadline may have cut this window's term/purpose queries
                # short: hold the source at it so the day is read again.
                _MHD_DEADLINE_CUT.add(display_name)
                _hold_freshness(f"{display_name} Health Dept", win_start.strftime('%Y-%m-%d'),
                                counted=False)
        else:
            raw_records = _mhd_fetch_window(session, path, win_start, win_end,
                                            sort=sort, label=display_name)
        if raw_records is None:
            if _MHD_PORTAL_BLOCKED or _past_deadline():
                # The breaker or the deadline: the group is reported
                # 'blocked'/'skipped' by the caller; ship what was read.
                log.error(f"  {display_name}: stopping early due to server errors on "
                          f"{win_start.strftime('%Y-%m-%d')}")
                stopped_early = True
                break
            # Neither: the window could not be read completely (exhausted
            # network retries, or sorted pages that did not fit together).
            # Fail closed — hold the source at the window so the next run
            # re-reads it, and raise so the group is reported, never ship
            # the jurisdiction short and call it 'ok'.
            _hold_window_start(f"{display_name} Health Dept", win_start.strftime('%Y-%m-%d'),
                               counted=False)
            raise RuntimeError(f"{display_name}: the search for "
                               f"{win_start:%Y-%m-%d}..{win_end:%Y-%m-%d} could not be read "
                               f"completely — nothing shipped for this jurisdiction")
        capped_queries += len(capped)

        new_count = win_program = win_city = win_excluded = 0
        for rec in raw_records:
            newest_seen = max(newest_seen, _mhd_row_date(rec))
            why = _mhd_row_rejection(rec, programs, city_allow, exclude)
            if why == 'program':
                win_program += 1
                continue
            if why == 'city':
                win_city += 1
                continue
            if why == 'excluded':
                win_excluded += 1
                continue
            parsed = _parse_mhd_record(rec, path, display_name, default_city,
                                       default_state, score_scale, metro)
            if parsed:
                uid = parsed.get('source_id') or f"{parsed['name']}|{parsed['inspection_date']}"
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    all_records.append(parsed)
                    new_count += 1
        dropped_by_program += win_program
        dropped_by_city += win_city
        dropped_by_exclude += win_excluded

        extra = ''
        if win_program or win_city or win_excluded:
            extra += (f"; dropped {win_program} by program, {win_city} by city, "
                      f"{win_excluded} excluded")
        if capped:
            extra += f"; {len(capped)} term/purpose queries INCOMPLETE at the cap"
        log.info(f"  Window {win_idx+1}/{len(windows)} "
                 f"({win_start.strftime('%Y-%m-%d')}..{win_end.strftime('%Y-%m-%d')}): "
                 f"{len(raw_records)} results ({new_count} new{extra}). Total: {len(all_records)}")

        time.sleep(MHD_DELAY)

    if dropped_by_program or dropped_by_city or dropped_by_exclude:
        log.info(f"{display_name}: filtered out {dropped_by_program} rows by program, "
                 f"{dropped_by_city} by city allowlist and {dropped_by_exclude} by the "
                 f"exclude patterns")
    if capped_queries:
        log.warning(f"{display_name}: {capped_queries} search queries hit the "
                    f"{MHD_QUERY_CAP} record cap — their buckets are incomplete and "
                    f"some inspections are missing this run")

    # A complete read that reached today tells what the source itself holds
    # (see _MHD_NEWEST_SEEN); a read the limit, the deadline or a failed
    # window cut short does not.
    limit_cut = bool(limit) and len(all_records) >= limit
    if not stopped_early and not limit_cut and range_end.date() >= datetime.now().date():
        _MHD_NEWEST_SEEN[display_name] = newest_seen

    if limit:
        all_records = all_records[:limit]

    if SKIP_KNOWN_INSPECTIONS and all_records:
        before = len(all_records)
        all_records = [
            r for r in all_records
            if (make_restaurant_id(r['name'], r['address'], r['city']),
                str(r['inspection_date'])[:10]) not in SKIP_KNOWN_INSPECTIONS
        ]
        if before - len(all_records):
            log.info(f"{display_name}: {before - len(all_records)}/{before} inspections "
                     f"already stored with details — not re-fetching their detail pages")

    # Scrape violation details from each inspection's public detail page
    if fetch_violations and all_records and _past_deadline():
        # Searched-but-undetailed records would ship looking violation-free.
        # Drop the run's records instead; the merge keeps existing data and
        # a future run re-attempts them.
        log.warning(f"{display_name}: fetch deadline reached before the detail "
                    f"phase — dropping {len(all_records)} undetailed inspections")
        _hold_freshness(all_records[0].get('source'),
                        min(str(r['inspection_date'])[:10] for r in all_records), counted=False)
        _MHD_DEADLINE_CUT.add(display_name)
        return []
    if fetch_violations and all_records:
        # Oldest first: a ban or deadline cut then drops the newest tail,
        # and the freshness hold re-reads from the first dropped day.
        inspections_with_id = sorted((r for r in all_records if r.get('source_id')),
                                     key=lambda r: str(r.get('inspection_date') or ''))
        log.info(f"{display_name}: fetching violation details for "
                 f"{len(inspections_with_id)} inspections "
                 f"({MHD_DETAIL_WORKERS} workers)...")
        fetched_violations, unreadable = _mhd_fetch_details_bulk(
            path, inspections_with_id,
            trust_official_score=score_scale == '100', label=display_name)
        log.info(f"{display_name}: {fetched_violations}/{len(inspections_with_id)} "
                 f"inspections had violation details")
        if unreadable:
            # An inspection whose observations could not be read must not
            # ship looking violation-free — drop it; a later run (or the
            # next weekly refresh) re-attempts it.
            log.warning(f"{display_name}: dropping {len(unreadable)} inspections "
                        f"with unreadable detail pages")
            # A block or the deadline left them unread: hold without
            # counting. Otherwise they were tried twice and failed.
            counted = not (_MHD_PORTAL_BLOCKED or _past_deadline())
            for r in unreadable:
                _hold_freshness(r.get('source'), str(r['inspection_date'])[:10], counted,
                                key=r.get('source_id'))
            if _past_deadline():
                _MHD_DEADLINE_CUT.add(display_name)
            elif counted and _mhd_detail_mass_failure(len(unreadable), len(inspections_with_id)):
                # Tried and failed on most of the pages: the detail pages
                # cannot be read (a moved path, a renamed slug, a new
                # template), which fetch_dfw reports so the run goes red
                # instead of quietly giving the records up in three runs.
                _MHD_DETAIL_FAILED[display_name] = (len(unreadable), len(inspections_with_id))
                log.error(f"{display_name}: {len(unreadable)}/{len(inspections_with_id)} detail "
                          f"pages unreadable — reporting the jurisdiction as failed")
            drop = {id(r) for r in unreadable}
            all_records = [r for r in all_records if id(r) not in drop]

    if stopped_early:
        log.warning(f"{display_name}: scrape stopped early — run again to get remaining data")

    log.info(f"{display_name}: {len(all_records)} total inspection records")
    return all_records


# MHD portals mix programs (food, swimming pools, septic, ...) in one search.
# Drop records that look like pool/spa inspections unless a food marker is
# also present ("Food + Liquid Waste" is a food program and must be kept).
_MHD_NON_FOOD_RE = re.compile(r'pool|swim|\bspa\b|septic', re.I)
_MHD_FOOD_RE = re.compile(r'food|restaurant|retail|grocer|meat|bakery|deli|market|caf', re.I)


def _mhd_is_food_record(rec):
    combined = ' '.join(str(rec.get(k) or '') for k in
                        ('programName', 'inspectionType', 'permitType'))
    if not combined.strip():
        return True  # no program info — keep, benefit of the doubt
    if _MHD_NON_FOOD_RE.search(combined) and not _MHD_FOOD_RE.search(combined):
        return False
    return True


def _mhd_row_date(rec):
    """A raw search row's inspection date as 'YYYY-MM-DD' ('' if none): ISO
    ('2026-10-20T00:00:00.000Z'), MM/DD/YYYY (the VDH districts) or an epoch
    in milliseconds (read as UTC), each through sane_inspection_date."""
    raw_date = rec.get('inspectionDate') or rec.get('date') or ''
    date_str = sane_inspection_date(raw_date) or ''
    if not date_str and raw_date:
        m = re.match(r'^(\d{2})/(\d{2})/(\d{4})', str(raw_date))
        if m:
            date_str = sane_inspection_date(f'{m.group(3)}-{m.group(1)}-{m.group(2)}') or ''
    if not date_str and raw_date:
        try:
            # UTC, as the Sunday runner reads it: a local-time read gives the
            # day before on a US clock.
            date_str = sane_inspection_date(
                datetime.fromtimestamp(int(raw_date) / 1000, tz=timezone.utc)
                .strftime('%Y-%m-%d')) or ''
        except Exception:
            date_str = ''
    return date_str


def _parse_mhd_record(rec, path, display_name, default_city, default_state,
                      score_scale='100', metro='DFW'):
    """Parse a single inspection record from the MyHealthDepartment API JSON
    response. `path` is the real portal path (_mhd_path) for source_url."""
    try:
        name = (rec.get('establishmentName') or '').strip()
        if not name or len(name) < 2:
            return None

        if not _mhd_is_food_record(rec):
            return None

        # Address fields
        addr1 = (rec.get('addressLine1') or '').strip()
        addr2 = (rec.get('addressLine2') or '').strip()
        address = ' '.join(filter(None, [addr1, addr2])).strip()
        city = (rec.get('city') or default_city).strip()
        state = (rec.get('state') or default_state).strip()
        if not re.fullmatch(r'[A-Za-z]{2}', state):
            state = default_state   # a spelled-out state would miss its coordinate box and split the city index
        zip_code = (rec.get('zip') or '').strip()

        # Inspection date — may be ISO string, MM/DD/YYYY (Virginia), or timestamp
        date_str = _mhd_row_date(rec)
        if not date_str:
            return None

        # Score
        score = rec.get('score')
        if score is not None:
            try:
                score = int(float(str(score)))
            except Exception:
                score = None

        if score_scale == 'none':
            # Portal publishes no score (e.g. Orange County) — neutral
            # placeholder; the violation-detail pass recomputes real risk.
            score = None
            risk_score = 70
        elif score is None:
            risk_score = 70
        elif score_scale == 'demerit':
            # Demerit points: lower is better. Rough mapping until the
            # detail scrape recomputes risk from actual violations.
            risk_score = max(0, 100 - 2 * score)
        else:
            risk_score = score

        # IDs for source URLs
        insp_id = str(rec.get('inspectionID') or rec.get('id') or '').strip()
        permit_id = str(rec.get('permitID') or '').strip()

        # Inspection type / purpose / the county's own result where it
        # publishes one (Arapahoe and Douglas County CO: 'Pass',
        # 'Re-Inspection Required', 'Closed'; most portals have no field)
        insp_type = (rec.get('inspectionType') or '').strip()
        purpose = (rec.get('purpose') or '').strip()
        result = str(rec.get('result') or '').strip()

        return {
            'name': name.title(),
            'address': address.title(),
            'city': city.title(),
            'state': state.upper(),
            'zip': zip_code,
            'latitude': rec.get('lat') or rec.get('latitude') or None,
            'longitude': rec.get('lng') or rec.get('longitude') or None,
            'inspection_date': date_str,
            'original_score': score,
            'risk_score': risk_score,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': insp_type,
            'purpose': purpose,
            'results': result,
            'source': f'{display_name} Health Dept',
            'source_url': f'https://inspections.myhealthdepartment.com/{path}/inspection/?inspectionID={insp_id}' if insp_id else f'https://inspections.myhealthdepartment.com/{path}',
            'source_id': insp_id or permit_id,
            'metro': metro,
        }
    except Exception as e:
        log.warning(f"{display_name}: failed to parse record: {e} — {str(rec)[:100]}")
        return None


# ─── ORANGE COUNTY, CA (MHD portal, newest-225 daily poll + sidecar) ────────
# Orange County Health Care Agency publishes on the MyHealthDepartment portal
# under the 'orange-county' slug, but the jurisdiction behaves unlike every
# other MHD portal this pipeline scrapes (measured 2026-10-10, ~140 calls):
#   * The search API IGNORES the date filter. Every searchInspections query
#     answers the newest inspections (inspectionDate DESC) truncated at the
#     portal's 225-record cap (start offsets 0..200; start >= 225 answers
#     {"err": true, "msg": "bad request"}). Any sort, searchStr or lat/lng
#     turns the query into a whole-database query, still capped. So the only
#     query that works is the empty one — programName "", filters
#     {"date": "", "purpose": ""}, searchStr "", sort {}, lat/lng 0 — read
#     as nine pages of 25: the newest 225 inspections.
#   * ~100-140 inspections are published per calendar day (~2-day lag), so
#     the newest 225 cover ~1.7 days. Polled DAILY (poll-orange-county.yml)
#     that captures essentially every inspection; a weekly poll would miss
#     ~75%. Each poll merges into a SIDECAR (OC_SIDECAR_PATH, one pipeline
#     record per line, the last OC_SIDECAR_KEEP_DAYS of inspections) and the
#     fetcher returns the sidecar's records, so the Sunday weekly run (data.js
#     + D1) sees the whole week even though no single poll does, and a poll
#     that fails ships what the sidecar already holds instead of nothing.
#   * The county publishes NO numeric score (score=0, scoreDisplay="" on every
#     record) — only an outcome: PASS, REINSPECTION, CLOSED or CLOSED-OWNER
#     INITIATED. The grade therefore IS the county's outcome, mapped to a
#     risk_score band (OC_RESULT_RISK); original_score stays None.
#   * The HTML detail page carries no violation text (jurisdiction config
#     showObservations=false). Violations exist only in the printable PDF
#     (print/?task=getPrintable&pKey=<inspectionID>, 0.3-1 MB), which is read
#     with the standard library: inflate the page content streams, decode the
#     Tj/TJ operators through the fonts' ToUnicode CMaps (Identity-H, 2-byte
#     CIDs) and lay the runs out by position. The PDF lists each cited item
#     as a bold "NN. Title - MAJ|MIN|OUT" header followed by "Inspector
#     Comments:" (the observation) and "Violation Description:" (the code
#     text); verified on four printables (0, 2, 3 and 8 items, one spanning a
#     page break) against pypdf. An inspection ships only once its printable
#     has been read: shipped without it, a PASS over "rodent droppings" would
#     grade A. Until then it waits in the sidecar and every poll retries it
#     (a PDF that parses without a Result counts as unread). Only after
#     OC_PDF_GIVE_UP_TRIES failed reads and OC_PDF_GIVE_UP_DAYS does it ship
#     with the county's official result alone. A poll where most printables
#     fail, or nearly all read with no violations, is reported as an error
#     (a layout change); a blocked printable endpoint as 'blocked'.

OC_SLUG = 'orange-county'
OC_DISPLAY_NAME = 'Orange County'
OC_SOURCE = 'Orange County Health Care Agency'
OC_METRO = 'Orange County'
# The poll's memory, committed by the daily workflow: one pipeline record per
# line plus the private '_polled' (first seen), '_result' (the portal's raw
# outcome) and '_pdf' (pending | ok | failed) fields, which never leave the
# fetcher. OC_SIDECAR_PATH in the environment overrides the location (tests,
# smoke runs).
OC_SIDECAR_PATH = (os.environ.get('OC_SIDECAR_PATH', '').strip()
                   or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'data', 'orange_county_inspections.jsonl'))
OC_SIDECAR_KEEP_DAYS = 10      # by inspection_date, relative to the newest held
# ...but never before the newest OC inspection the last COMMITTED weekly run
# shipped into data.js (its recorded latest), up to this bound: a weekly run
# whose D1 update failed holds data.js back, and the days it held must still
# be in the sidecar for next week's run to ship.
OC_SIDECAR_MAX_DAYS = 35
# Printable-PDF pace: a small pool, paced, and the phase stops at the first
# 403/429 that survives a retry (the records still ship, see above). The cap
# bounds a run that finds a long backlog of retries.
OC_PDF_WORKERS = int(os.environ.get('OC_PDF_WORKERS', '2'))
OC_PDF_DELAY = float(os.environ.get('OC_PDF_DELAY', '0.5'))
OC_PDF_MAX_PER_RUN = int(os.environ.get('OC_PDF_MAX_PER_RUN', '400'))
# An inspection ships only once its printable has been read; one whose PDF
# has failed OC_PDF_GIVE_UP_TRIES times AND was first polled this many days
# ago ships with the county's official result alone (never one whose PDF
# was simply not tried, e.g. while the endpoint was blocked).
OC_PDF_GIVE_UP_DAYS = 5
OC_PDF_GIVE_UP_TRIES = 3
# A poll in which most printables fail to parse (at least this many tried)
# means the county changed the PDF layout: the run is reported as an error.
OC_PDF_MASS_FAILURE_MIN = 20
# Orange County publishes only pass / reinspection / closure, so the grade is
# the county's outcome, not a point score:
#   PASS                    -> 90 (A)  routine inspection passed
#   REINSPECTION            -> 75 (C)  violations warrant a return visit
#   CLOSED                  -> 50 (F)  permit suspended, facility closed
#   CLOSED-OWNER INITIATED  -> 50 (F)  closed at the operator's request
# Any other outcome (none seen in ~900 records) keeps the neutral 70 with its
# raw outcome text as the result, and is logged.
OC_RESULT_RISK = {'PASS': 90, 'REINSPECTION': 75, 'CLOSED': 50,
                  'CLOSED-OWNER INITIATED': 50}
OC_RESULT_LABELS = {'PASS': 'Pass', 'REINSPECTION': 'Reinspection required',
                    'CLOSED': 'Closed', 'CLOSED-OWNER INITIATED': 'Closed (owner initiated)'}
OC_NEUTRAL_RISK = 70
# The PDF tags every cited item with the county's own tier: MAJ (major risk
# factor), MIN (minor risk factor) or OUT (good-retail-practice item out of
# compliance). The tag sets the stored severity; classify_violation only
# supplies the category. Jev treats the source's tiers as fixed.
OC_TAG_SEVERITY = {'MAJ': 'priority', 'MIN': 'priority_foundation', 'OUT': 'core'}
_OC_PDF_BLOCKED = False      # printable endpoint 403/429'd through a retry this run
_OC_LAST_POLL = {}           # this run's poll outcome, read by main() for _record_group


# --- stdlib PDF text extraction (enough for the portal's printables) ---------

_PDF_OBJ_RE = re.compile(rb'(\d+)\s+0\s+obj\b(.*?)endobj', re.S)
_PDF_STREAM_RE = re.compile(rb'stream\r?\n(.*?)\r?\nendstream', re.S)
_PDF_REF_RE = re.compile(rb'(\d+)\s+0\s+R')
_PDF_NAME_REF_RE = re.compile(rb'/([^\s/\[\]<>()]+)\s+(\d+)\s+0\s+R')
_PDF_TEXT_OP_RE = re.compile(
    rb'/(?P<font>[^\s/\[\]<>()]+)\s+(?P<size>-?[\d.]+)\s+Tf'
    rb'|(?P<tm>(?:-?[\d.]+\s+){6})Tm'
    rb'|(?P<td>-?[\d.]+\s+-?[\d.]+)\s+T[dD]'
    rb'|\[(?P<arr>(?:[^\]\\]|\\.)*)\]\s*TJ'
    rb'|<(?P<hex>[0-9A-Fa-f\s]*)>\s*Tj'
    rb'|\((?P<lit>(?:[^()\\]|\\.)*)\)\s*Tj')
_PDF_STRING_RE = re.compile(rb'<([0-9A-Fa-f\s]*)>|\(((?:[^()\\]|\\.)*)\)')
_OC_PDF_ESCAPES = {b'n': b'\n', b'r': b'\r', b't': b'\t', b'b': b'\b', b'f': b'\f',
                b'(': b'(', b')': b')', b'\\': b'\\'}


def _pdf_objects(data):
    return {int(m.group(1)): m.group(2) for m in _PDF_OBJ_RE.finditer(data)}


def _pdf_stream(body):
    """Decoded bytes of an object's stream (FlateDecode or raw), or None."""
    m = _PDF_STREAM_RE.search(body)
    if not m:
        return None
    raw = m.group(1)
    if b'/FlateDecode' not in body[:m.start()]:
        return raw
    try:
        return zlib.decompress(raw)
    except zlib.error:
        try:
            return zlib.decompressobj().decompress(raw)   # tolerate a truncated stream
        except zlib.error:
            return None


def _pdf_tounicode(cmap):
    """CID -> text from a ToUnicode CMap (bfchar and both bfrange forms)."""
    out = {}
    if not cmap:
        return out

    def utf16(h):
        try:
            return bytes.fromhex(h.decode()).decode('utf-16-be', 'replace')
        except ValueError:
            return ''
    for blk in re.findall(rb'beginbfchar(.*?)endbfchar', cmap, re.S):
        for src, dst in re.findall(rb'<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>', blk):
            out[int(src, 16)] = utf16(dst)
    for blk in re.findall(rb'beginbfrange(.*?)endbfrange', cmap, re.S):
        for lo, hi, arr in re.findall(rb'<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*\[(.*?)\]', blk, re.S):
            lo = int(lo, 16)
            for i, d in enumerate(re.findall(rb'<([0-9A-Fa-f]+)>', arr)):
                out[lo + i] = utf16(d)
        for lo, hi, dst in re.findall(
                rb'<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>(?!\s*>)', blk):
            lo, hi, base = int(lo, 16), int(hi, 16), int(dst, 16)
            for c in range(lo, min(hi, lo + 65535) + 1):
                if base + c - lo < 0x110000:
                    out.setdefault(c, chr(base + c - lo))
    return out


def _pdf_fonts(objs):
    """Font object number -> (cid->text map, or None for a simple font; bold?)."""
    fonts = {}
    for num, body in objs.items():
        if not re.search(rb'/Type\s*/Font\b', body) or re.search(rb'/Subtype\s*/CIDFontType', body):
            continue
        base = re.search(rb'/BaseFont\s*/([^\s/>]+)', body)
        bold = bool(base and re.search(rb'bold', base.group(1), re.I))
        cmap = None
        if re.search(rb'/Subtype\s*/Type0\b', body):
            cmap = {}
            tu = re.search(rb'/ToUnicode\s+(\d+)\s+0\s+R', body)
            if tu and int(tu.group(1)) in objs:
                cmap = _pdf_tounicode(_pdf_stream(objs[int(tu.group(1))]))
        fonts[num] = (cmap, bold)
    return fonts


def _pdf_page_order(objs):
    """Page object numbers in document order (via the Pages tree)."""
    pages = [n for n, b in objs.items() if re.search(rb'/Type\s*/Page\b', b)]
    roots = [n for n, b in objs.items()
             if re.search(rb'/Type\s*/Pages\b', b) and b'/Parent' not in b]
    ordered = []

    def walk(num, depth=0):
        body = objs.get(num)
        if body is None or depth > 32 or num in ordered:
            return
        if re.search(rb'/Type\s*/Page\b', body):
            ordered.append(num)
            return
        kids = re.search(rb'/Kids\s*\[(.*?)\]', body, re.S)
        if kids:
            for k in _PDF_REF_RE.findall(kids.group(1)):
                walk(int(k), depth + 1)
    for r in roots:
        walk(r)
    return ordered or sorted(pages)


def _pdf_dict(objs, body, key):
    """The dictionary at /<key> in body, inline or through an indirect ref."""
    m = re.search(rb'/' + key + rb'\s+(\d+)\s+0\s+R', body)
    if m:
        return objs.get(int(m.group(1)), b'')
    m = re.search(rb'/' + key + rb'\s*<<', body)
    if not m:
        return b''
    i, depth, start = m.end() - 2, 0, m.end() - 2
    while i < len(body):
        if body.startswith(b'<<', i):
            depth += 1
            i += 2
        elif body.startswith(b'>>', i):
            depth -= 1
            i += 2
            if depth == 0:
                return body[start:i]
        else:
            i += 1
    return body[start:]


def _pdf_decode_string(raw, is_hex, cmap):
    """Text of one string operand. Type0 fonts (cmap given) carry 2-byte CIDs
    mapped through ToUnicode; simple fonts are read as Latin-1."""
    if is_hex:
        h = re.sub(rb'\s+', b'', raw).decode('ascii', 'ignore')
        if cmap is None:
            try:
                return bytes.fromhex(h[:len(h) - len(h) % 2]).decode('latin-1')
            except ValueError:
                return ''
        return ''.join(cmap.get(int(h[i:i + 4], 16), '�')
                       for i in range(0, len(h) - len(h) % 4, 4))

    def unescape(m):
        e = m.group(1)
        return _OC_PDF_ESCAPES.get(e) or bytes([int(e, 8) & 0xFF])
    b = re.sub(rb'\\([nrtbf()\\]|[0-7]{1,3})', unescape, raw)
    if cmap is None:
        return b.decode('latin-1')
    return ''.join(cmap.get(int.from_bytes(b[i:i + 2], 'big'), '�')
                   for i in range(0, len(b) - len(b) % 2, 2))


def _oc_pdf_text_rows(data, y_tolerance=3.0):
    """Text of each page as rows of runs, top to bottom and left to right:
    [[(y, x, bold, text), ...] per row] per page. Each BT..ET block yields one
    run per string operator, positioned by its Tm (plus Td/TD offsets); runs
    whose baselines agree within y_tolerance form a row. Returns [] for
    anything that is not a PDF."""
    if not data or not data.lstrip()[:5].startswith(b'%PDF'):
        return []
    objs = _pdf_objects(data)
    fonts = _pdf_fonts(objs)
    root_pages = [b for b in objs.values() if re.search(rb'/Type\s*/Pages\b', b)]
    pages = []
    for pn in _pdf_page_order(objs):
        body = objs[pn]
        res = _pdf_dict(objs, body, b'Resources')
        if not res and root_pages:
            res = _pdf_dict(objs, root_pages[0], b'Resources')
        fmap = {n: int(r) for n, r in _PDF_NAME_REF_RE.findall(_pdf_dict(objs, res, b'Font'))}
        m = re.search(rb'/Contents\s+(\d+)\s+0\s+R', body)
        arr = re.search(rb'/Contents\s*\[(.*?)\]', body, re.S)
        refs = ([int(m.group(1))] if m else
                [int(x) for x in _PDF_REF_RE.findall(arr.group(1))] if arr else [])
        content = b'\n'.join(_pdf_stream(objs[r]) or b'' for r in refs if r in objs)
        runs = []
        for blk in re.findall(rb'BT(.*?)ET', content, re.S):
            x = y = 0.0
            cmap, bold = None, False
            for op in _PDF_TEXT_OP_RE.finditer(blk):
                if op.group('font') is not None:
                    cmap, bold = fonts.get(fmap.get(op.group('font')), (None, False))
                elif op.group('tm') is not None:
                    nums = op.group('tm').split()
                    x, y = float(nums[4]), float(nums[5])
                elif op.group('td') is not None:
                    dx, dy = op.group('td').split()
                    x, y = x + float(dx), y + float(dy)
                else:
                    if op.group('arr') is not None:
                        parts = _PDF_STRING_RE.findall(op.group('arr'))
                    elif op.group('hex') is not None:
                        parts = [(op.group('hex'), b'')]
                    else:
                        parts = [(b'', op.group('lit'))]
                    text = ''.join(_pdf_decode_string(h if h else l, bool(h), cmap)
                                   for h, l in parts if h or l)
                    if text.strip():
                        runs.append((y, x, bold, text))
        runs.sort(key=lambda r: (-r[0], r[1]))
        rows, cur, cur_y = [], [], None
        for r in runs:
            if cur and abs(r[0] - cur_y) > y_tolerance:
                rows.append(sorted(cur, key=lambda q: q[1]))
                cur = []
            if not cur:
                cur_y = r[0]
            cur.append(r)
        if cur:
            rows.append(sorted(cur, key=lambda q: q[1]))
        pages.append(rows)
    return pages


# --- the printable's structure ---------------------------------------------

_OC_PDF_PAGE_LABELS = {'establishment name', 'inspection date', 'establishment address',
                       'permit number', 'complaint establishment name',
                       'complaint establishment address'}
_OC_VIOLATION_HEADER_RE = re.compile(r'^(\d{1,3})\.\s+(.+?)\s+-\s+([A-Z][A-Z/]{1,4})$')
_OC_MARKER_RE = re.compile(r'^[A-Z][A-Z/&]{1,5}$')      # e.g. a standalone bold COS


def _oc_parse_printable(pdf_bytes):
    """Read an Orange County printable inspection report.

    Returns {'result', 'inspection_type', 'inspection_date' (MM/DD/YYYY as
    printed), 'violations': [{'code', 'title', 'tag', 'observation',
    'description', 'flags'}], 'general_comments', 'pages'} or None when the
    bytes are not a readable PDF. Page 1 is the cover (labels with their
    values printed beneath them); the "Iterative - Violations" section lists
    each cited item as a bold "NN. Title - TAG" header followed by the
    inspector's observation ("Inspector Comments:") and the code text
    ("Violation Description:"), continuing across page breaks (every page
    repeats a header block of labels + values, which is skipped). A closure
    reinspection can have no itemized violations at all; its narrative is in
    "General Inspection Comments"."""
    pages = _oc_pdf_text_rows(pdf_bytes)
    if not pages:
        return None
    out = {'result': '', 'inspection_type': '', 'inspection_date': '',
           'violations': [], 'general_comments': '', 'pages': len(pages)}
    cover = [r for row in pages[0] for r in row]

    def value_under(label):
        for y, x, bold, text in cover:
            if bold and text.strip().lower() == label:
                below = [(y - ly, t.strip()) for ly, lx, b, t in cover
                         if not b and t.strip() and abs(lx - x) <= 12 and 0 < y - ly <= 40]
                if below:
                    return min(below)[1]
        return ''
    out['result'] = value_under('result')
    out['inspection_type'] = value_under('inspection type')
    out['inspection_date'] = value_under('inspection date')

    section, skip_next, cur, field = None, False, None, 'observation'
    for page in pages:
        for row in page:
            texts = [t.strip() for _, _, _, t in row if t.strip()]
            if not texts:
                continue
            if skip_next:
                skip_next = False
                continue
            bold = all(b for _, _, b, t in row if t.strip())
            if bold and all(t.lower() in _OC_PDF_PAGE_LABELS for t in texts):
                skip_next = True        # a page header: labels, then their values
                continue
            line = ' '.join(texts)
            low = line.lower()
            if bold and low == 'iterative - violations':
                section = 'violations'
                continue
            if bold and low == 'general inspection comments':
                section, cur = 'comments', None
                continue
            if section == 'violations':
                if bold:
                    m = _OC_VIOLATION_HEADER_RE.match(line)
                    if m:
                        cur = {'code': m.group(1), 'title': ' '.join(m.group(2).split()),
                               'tag': m.group(3), 'observation': '', 'description': '',
                               'flags': []}
                        out['violations'].append(cur)
                        field = 'observation'
                    elif cur is not None and _OC_MARKER_RE.match(line):
                        cur['flags'].append(line)
                    else:
                        section, cur = 'other', None     # the next section's title
                    continue
                if cur is None:
                    continue
                if low.startswith('inspector comments:'):
                    field = 'observation'
                    line = line[len('inspector comments:'):].strip()
                elif low.startswith('violation description:'):
                    field = 'description'
                    line = line[len('violation description:'):].strip()
                if line:
                    cur[field] = f"{cur[field]} {line}".strip()
            elif section == 'comments':
                if bold:
                    section = 'other'
                    continue
                out['general_comments'] = f"{out['general_comments']} {line}".strip()
    return out


def _oc_violation_rows(parsed):
    """[category, severity, description] rows for a parsed printable. The
    county's tag fixes the severity; the text is the cited item followed by
    the inspector's observation (the code text when there is none)."""
    rows = []
    for v in (parsed or {}).get('violations') or []:
        observation = v.get('observation') or v.get('description') or ''
        text = f"{v['code']}. {v['title']}"
        if v.get('tag'):
            text += f" ({v['tag']})"
        if observation:
            text += f": {observation}"
        category, severity, _ = classify_violation(f"{v['title']} {observation}")
        severity = OC_TAG_SEVERITY.get((v.get('tag') or '').upper(), severity)
        rows.append([category, severity, text[:500]])
    return rows


# --- records, sidecar, poll ---------------------------------------------------

def _oc_printable_url(inspection_id):
    return (f'{MHD_BASE_URL}{OC_SLUG}/print/?task=getPrintable&path={OC_SLUG}'
            f'&pKey={inspection_id}')


def _oc_result_fields(result):
    """(risk_score, results label) for the portal's raw outcome string."""
    result = ' '.join(str(result or '').upper().split())
    if result in OC_RESULT_RISK:
        return OC_RESULT_RISK[result], OC_RESULT_LABELS[result]
    return OC_NEUTRAL_RISK, result.title()


def _oc_build_record(rec):
    """Pipeline record for one search-API row (None if unusable)."""
    parsed = _parse_mhd_record(rec, OC_SLUG, OC_DISPLAY_NAME, 'Anaheim', 'CA',
                               score_scale='none', metro=OC_METRO)
    insp_id = str(rec.get('inspectionID') or '').strip()
    if not parsed or not insp_id:
        return None
    # A third of the portal's establishment names end in a caret ("Disneyland^",
    # "Ami Sushi ^"), a data-entry marker with no public meaning; it would
    # otherwise show on the map and sit inside the restaurant id.
    parsed['name'] = parsed['name'].replace('^', '')
    for k in ('name', 'address', 'city'):
        parsed[k] = ' '.join(str(parsed.get(k) or '').split())
    if len(parsed['name']) < 2:
        return None
    result = ' '.join(str(rec.get('result') or '').upper().split())
    risk, label = _oc_result_fields(result)
    parsed.update({
        'original_score': None,
        'risk_score': risk,
        'results': label,
        'zip': (parsed.get('zip') or '').strip()[:5],
        'source': OC_SOURCE,
        'source_url': _oc_printable_url(insp_id),
        'source_id': insp_id,
        'metro': OC_METRO,
        '_result': result,
    })
    return parsed


def _oc_load_sidecar(path):
    recs = {}
    try:
        with open(path) as f:
            for n, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    log.warning(f"Orange County: sidecar line {n} is not JSON — skipped")
                    continue
                sid = str(r.get('source_id') or '')
                if sid and r.get('inspection_date'):
                    recs[sid] = r
    except FileNotFoundError:
        log.info(f"Orange County: no sidecar at {path} yet — starting empty")
    return recs


def _oc_write_sidecar(path, recs):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w') as f:
        for r in sorted(recs.values(), key=lambda r: (r['inspection_date'], r['source_id'])):
            f.write(json.dumps(r, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    os.replace(tmp, path)


def _oc_prune(recs):
    """Drop inspections older than OC_SIDECAR_KEEP_DAYS before the newest held
    (relative to the data, not the clock, so a quiet portal never empties the
    sidecar), keeping everything after the last shipped weekly (see
    OC_SIDECAR_MAX_DAYS). Returns the number dropped."""
    if not recs:
        return 0
    newest = max(str(r['inspection_date'])[:10] for r in recs.values())
    newest_day = datetime.strptime(newest, '%Y-%m-%d')
    cutoff = (newest_day - timedelta(days=OC_SIDECAR_KEEP_DAYS)).strftime('%Y-%m-%d')
    shipped = _group_latest('Orange County')
    if shipped and shipped < cutoff:
        cutoff = max(shipped, (newest_day - timedelta(days=OC_SIDECAR_MAX_DAYS)).strftime('%Y-%m-%d'))
    old = [sid for sid, r in recs.items() if str(r['inspection_date'])[:10] < cutoff]
    for sid in old:
        del recs[sid]
    return len(old)


def _oc_poll_newest(session, max_pages=None):
    """Read the newest inspections: the empty query, MHD_QUERY_CAP /
    MHD_PAGE_SIZE pages of MHD_PAGE_SIZE (a short page ends it early).
    Returns (raw_records, complete, reason) where reason is '' or why the
    read stopped: 'blocked', 'deadline' or 'error'."""
    pages = MHD_QUERY_CAP // MHD_PAGE_SIZE
    if max_pages:
        pages = max(1, min(pages, max_pages))
    records = []
    for i in range(pages):
        if _MHD_PORTAL_BLOCKED:
            return records, False, 'blocked'
        if _past_deadline():
            return records, False, 'deadline'
        if i:
            time.sleep(MHD_DELAY)
        page = _mhd_search_page(session, OC_SLUG, '', i * MHD_PAGE_SIZE)
        if page is None:
            return records, False, 'blocked' if _MHD_PORTAL_BLOCKED else 'error'
        records.extend(page)
        if len(page) < MHD_PAGE_SIZE:
            break
    return records, True, ''


def _oc_fetch_printable(session, inspection_id, retry_count=0):
    """Bytes of the printable PDF for one inspection, or None. A 403/429 is
    retried once; a second one stops this run's PDF phase (the records still
    ship with their official results) without touching the search breaker."""
    global _OC_PDF_BLOCKED
    if _MHD_PORTAL_BLOCKED or _OC_PDF_BLOCKED:
        return None
    try:
        r = session.get(f'{MHD_BASE_URL}{OC_SLUG}/print/',
                        params={'task': 'getPrintable', 'path': OC_SLUG, 'pKey': inspection_id},
                        timeout=60)
    except requests.RequestException as e:
        # Timeouts and resets happen a few times per hundred over a long
        # phase (18 of 225 on 2026-10-10, all fine on re-fetch): one quick
        # retry, then leave it for the next poll.
        if retry_count < 1 and not _past_deadline():
            time.sleep(2)
            return _oc_fetch_printable(session, inspection_id, retry_count + 1)
        log.debug(f"  Orange County printable {inspection_id} failed: {e}")
        return None
    if r.status_code in (403, 429):
        if retry_count < 1 and not _past_deadline():
            time.sleep(MHD_RETRY_DELAY)
            return _oc_fetch_printable(session, inspection_id, retry_count + 1)
        if not _OC_PDF_BLOCKED:
            _OC_PDF_BLOCKED = True
            log.warning(f"  Orange County: printable PDFs blocked ({r.status_code}) through a "
                        f"retry — stopping this run's PDF phase; the inspections ship with "
                        f"their official results and the PDFs are retried next poll")
        return None
    if r.status_code != 200 or not r.content.lstrip()[:5].startswith(b'%PDF'):
        log.debug(f"  Orange County printable {inspection_id}: HTTP {r.status_code}, "
                  f"{r.headers.get('content-type')}")
        return None
    return r.content


def _oc_fetch_pdfs_bulk(records):
    """Fetch and parse the printable for each record (OC_PDF_WORKERS workers,
    OC_PDF_DELAY between a worker's fetches), filling the violation fields in
    place and marking '_pdf' ok|failed. Records skipped (blocked, deadline)
    keep their previous mark so the next poll retries them.
    Returns (ok, failed, skipped)."""
    local = threading.local()

    def get_session():
        if not hasattr(local, 'session'):
            local.session = _mhd_session()
        return local.session

    def one(rec):
        if _OC_PDF_BLOCKED or _MHD_PORTAL_BLOCKED or _past_deadline():
            return rec, None, 'skipped'
        data = _oc_fetch_printable(get_session(), rec['source_id'])
        if not (_OC_PDF_BLOCKED or _MHD_PORTAL_BLOCKED):
            time.sleep(OC_PDF_DELAY)
        if data is None:
            return rec, None, 'skipped' if _OC_PDF_BLOCKED else 'failed'
        try:
            parsed = _oc_parse_printable(data)
        except Exception as e:      # a malformed PDF must never take the run down
            log.debug(f"  Orange County printable {rec['source_id']} unreadable: {e}")
            parsed = None
        # A PDF that parses but carries no Result is not an OC printable we
        # understand (a layout change, an error page rendered as PDF): read
        # as zero violations it would ship looking clean, so it counts as
        # failed and is retried by the next poll.
        if parsed is not None and not parsed.get('result'):
            log.debug(f"  Orange County printable {rec['source_id']}: no Result found — "
                      f"unrecognised layout")
            parsed = None
        return rec, parsed, 'ok' if parsed is not None else 'failed'

    ok = failed = skipped = 0
    with ThreadPoolExecutor(max_workers=OC_PDF_WORKERS) as pool:
        for rec, parsed, status in pool.map(one, records):
            if status == 'ok':
                rows = _oc_violation_rows(parsed)
                _, pv, pfv, cv = calc_risk_score(rows)
                rec.update(violations=rows, priority_violations=pv,
                           priority_foundation_violations=pfv, core_violations=cv,
                           total_violations=len(rows), _pdf='ok')
                pdf_result = ' '.join((parsed.get('result') or '').upper().split())
                if pdf_result and rec.get('_result') and pdf_result != rec['_result']:
                    log.debug(f"  Orange County {rec['source_id']}: PDF result {pdf_result!r} "
                              f"vs search {rec['_result']!r}")
                ok += 1
            elif status == 'failed':
                rec['_pdf'] = 'failed'
                rec['_pdf_tries'] = (rec.get('_pdf_tries') or 0) + 1
                failed += 1
            else:
                skipped += 1
    return ok, failed, skipped


def _oc_group_outcome():
    """(status, detail) for _record_group from this run's poll: (None, note)
    when the poll worked, else the failure the daily job should fail on even
    though the sidecar's records still shipped."""
    status = _OC_LAST_POLL.get('status') or 'ok'
    return (None if status == 'ok' else status), _OC_LAST_POLL.get('detail', '')


def fetch_orange_county(since_date=None, limit=None, fetch_violations=True,
                        sidecar_path=None, geocode=None):
    """
    Orange County, CA (Anaheim, Santa Ana, Irvine, Huntington Beach, ...):
    poll the MyHealthDepartment portal's newest 225 inspections, merge them
    into the sidecar and return the sidecar's inspections dated on or after
    since_date (all of them when None). See the section comment for why the
    poll is shaped this way.

    Each inspection's grade is the county's official outcome (OC_RESULT_RISK:
    PASS 90, REINSPECTION 75, CLOSED / CLOSED-OWNER INITIATED 50) — the
    county publishes no numeric score, so original_score is None and the
    result is never inferred from the violation details. Violations come
    from the printable PDF (source_url, the only public place they exist),
    fetched for every inspection new to the sidecar and retried for those
    whose PDF failed before. An inspection ships only once its printable
    has been read (a PDF that parses but carries no Result counts as
    unread); until then it waits in the sidecar and every poll retries it.
    One whose printable is still unreadable OC_PDF_GIVE_UP_DAYS after it was
    first polled ships with the county's official result and no violation
    list — the result is the county's own outcome, not something inferred
    from the details. A poll in which most printables fail is reported as
    an error (a layout change would otherwise hold everything back quietly).

    The portal has no coordinates: `geocode` (geocode_missing_coords in
    main()) is applied only to the sidecar's records that still lack them,
    and the coordinates are stored in the sidecar, so a restaurant is
    geocoded once, not every day. `limit` (test mode) bounds both the pages
    polled and the records returned. Honours _past_deadline() and
    _MHD_PORTAL_BLOCKED; if the poll fails the sidecar's records are still
    returned (and _oc_group_outcome() tells main() so the run is reported
    as failed) — a bad day never loses the week.
    """
    path = sidecar_path or OC_SIDECAR_PATH
    today = datetime.now().strftime('%Y-%m-%d')
    sidecar = _oc_load_sidecar(path)
    log.info(f"Fetching Orange County (newest-{MHD_QUERY_CAP} poll; sidecar {path}: "
             f"{len(sidecar)} inspections; since={since_date}, limit={limit})")
    poll = {'status': 'ok', 'detail': '', 'pages': 0, 'raw': 0, 'new': 0, 'updated': 0,
            'pdf_tried': 0, 'pdf_ok': 0, 'pdf_failed': 0, 'pdf_skipped': 0,
            'geocoded': 0, 'geocode_missing': 0, 'pruned': 0, 'sidecar': 0}
    _OC_LAST_POLL.clear()
    _OC_LAST_POLL.update(poll)

    new_records = []
    unknown_results = set()
    if _MHD_PORTAL_BLOCKED:
        poll.update(status='blocked', detail=f"MHD portal blocked this run; shipped the "
                                             f"sidecar's {len(sidecar)} inspections")
        log.warning("Orange County: poll skipped — MHD portal is blocking this run")
    elif _past_deadline():
        poll.update(status='skipped', detail=f"fetch deadline reached before the poll; "
                                             f"shipped the sidecar's {len(sidecar)} inspections")
        log.warning("Orange County: poll skipped — pipeline fetch deadline reached")
    else:
        session = _mhd_session()
        max_pages = math.ceil(limit / MHD_PAGE_SIZE) if limit else None
        raw, complete, reason = _oc_poll_newest(session, max_pages)
        poll['raw'] = len(raw)
        poll['pages'] = math.ceil(len(raw) / MHD_PAGE_SIZE)
        for rec in raw:
            built = _oc_build_record(rec)
            if not built:
                continue
            if built['_result'] not in OC_RESULT_RISK:
                unknown_results.add(built['_result'] or '(blank)')
            existing = sidecar.get(built['source_id'])
            if existing is None:
                built['_polled'] = today
                built['_pdf'] = 'pending'
                sidecar[built['source_id']] = built
                new_records.append(built)
            else:
                # Refresh what the portal states about the visit; keep what
                # earlier polls earned (coordinates, violations, first-seen).
                for k in ('inspection_type', 'purpose', 'results', 'risk_score', '_result'):
                    existing[k] = built[k]
                poll['updated'] += 1
        poll['new'] = len(new_records)
        if not raw:
            status = {'blocked': 'blocked', 'deadline': 'skipped'}.get(reason, 'error')
            poll.update(status=status,
                        detail=f"poll {reason or 'failed'}: no page read; shipped the "
                               f"sidecar's {len(sidecar)} inspections")
            log.error(f"Orange County: {poll['detail']}")
        elif not complete:
            poll['detail'] = (f"poll read {poll['pages']} of {MHD_QUERY_CAP // MHD_PAGE_SIZE} "
                              f"pages ({reason}); {len(new_records)} new inspections")
            log.warning(f"Orange County: {poll['detail']}")
        else:
            log.info(f"Orange County: poll read {len(raw)} inspections in {poll['pages']} pages "
                     f"— {len(new_records)} new, {poll['updated']} already held")
    if unknown_results:
        log.warning(f"Orange County: unmapped outcome(s) {sorted(unknown_results)} — "
                    f"recorded with the neutral risk {OC_NEUTRAL_RISK}; extend OC_RESULT_RISK")

    # The mapping is policy, not data: re-derive it for every held record so a
    # change to OC_RESULT_RISK reaches the inspections already in the sidecar.
    for r in sidecar.values():
        if r.get('_result'):
            r['risk_score'], r['results'] = _oc_result_fields(r['_result'])
    poll['pruned'] = _oc_prune(sidecar)

    if fetch_violations and poll['status'] == 'ok':
        pending = [r for r in sidecar.values() if r.get('_pdf') in (None, 'pending', 'failed')]
        # New inspections before retries, newest first within each (two stable sorts).
        pending.sort(key=lambda r: (r['inspection_date'], r['source_id']), reverse=True)
        pending.sort(key=lambda r: 0 if r.get('_pdf') == 'pending' else 1)
        if len(pending) > OC_PDF_MAX_PER_RUN:
            log.warning(f"Orange County: {len(pending)} printables to fetch — capping at "
                        f"{OC_PDF_MAX_PER_RUN} this run")
            pending = pending[:OC_PDF_MAX_PER_RUN]
        if pending and not _past_deadline():
            log.info(f"Orange County: fetching {len(pending)} printable PDFs "
                     f"({OC_PDF_WORKERS} workers)...")
            ok, failed, skipped = _oc_fetch_pdfs_bulk(pending)
            poll.update(pdf_tried=len(pending), pdf_ok=ok, pdf_failed=failed, pdf_skipped=skipped)
            log.info(f"Orange County: {ok}/{len(pending)} printables read "
                     f"({failed} failed, {skipped} skipped)")
            read = [r for r in pending if r.get('_pdf') == 'ok']
            empty = sum(1 for r in read if not r.get('violations'))
            if ok + failed >= OC_PDF_MASS_FAILURE_MIN and failed > (ok + failed) / 2:
                poll.update(status='error',
                            detail=f"{failed} of {ok + failed} printables unreadable — "
                                   f"has the county changed the PDF layout?")
                log.error(f"Orange County: {poll['detail']}")
            elif len(read) >= OC_PDF_MASS_FAILURE_MIN and empty > 0.9 * len(read):
                # About a fifth of real printables cite nothing; nearly all
                # of them reading as clean means the violations section is
                # no longer where the parser looks.
                poll.update(status='error',
                            detail=f"{empty} of {len(read)} printables read with no violations — "
                                   f"has the county changed the violations layout?")
                log.error(f"Orange County: {poll['detail']}")
            elif skipped and (_OC_PDF_BLOCKED or _MHD_PORTAL_BLOCKED or _past_deadline()):
                blocked = _OC_PDF_BLOCKED or _MHD_PORTAL_BLOCKED
                why = ('printable endpoint blocked' if _OC_PDF_BLOCKED else
                       'MHD portal blocked' if _MHD_PORTAL_BLOCKED else 'fetch deadline reached')
                poll.update(status='blocked' if blocked else 'skipped',
                            detail='; '.join(x for x in (
                                poll.get('detail'),
                                f"{why}; {skipped} inspections wait for their printable") if x))
                log.warning(f"Orange County: {poll['detail']}")
    elif fetch_violations and new_records:
        log.warning(f"Orange County: PDF phase skipped (poll {poll['status']}); "
                    f"{len(new_records)} new inspections wait in the sidecar for their printable")

    missing = [r for r in sidecar.values() if not r.get('latitude') or not r.get('longitude')]
    poll['geocode_missing'] = len(missing)
    if geocode is not None and missing:
        geocode(missing)
        drop_implausible_coords(missing)     # never store a wrong pin for ten days
        poll['geocoded'] = sum(1 for r in missing if r.get('latitude') and r.get('longitude'))
        log.info(f"Orange County: geocoded {poll['geocoded']}/{len(missing)} inspections "
                 f"without coordinates")

    _oc_write_sidecar(path, sidecar)
    poll['sidecar'] = len(sidecar)

    since = str(since_date)[:10] if since_date else ''
    give_up = (datetime.now() - timedelta(days=OC_PDF_GIVE_UP_DAYS)).strftime('%Y-%m-%d')
    out = []
    held_back = given_up = 0
    for r in sorted(sidecar.values(), key=lambda r: (r['inspection_date'], r['source_id']),
                    reverse=True):
        if since and str(r['inspection_date'])[:10] < since:
            continue
        if fetch_violations and r.get('_pdf') != 'ok':
            # Not read yet: the printable is the only place the violations
            # are, and an inspection shipped without them can grade A over
            # rodent droppings. It stays in the sidecar for the next poll —
            # unless the daily polls have failed on it for OC_PDF_GIVE_UP_DAYS,
            # when it ships with the county's official result alone.
            if ((r.get('_polled') or today) > give_up
                    or (r.get('_pdf_tries') or 0) < OC_PDF_GIVE_UP_TRIES):
                held_back += 1
                continue
            given_up += 1
        out.append({k: v for k, v in r.items() if not k.startswith('_')})
    if held_back or given_up:
        log.info(f"Orange County: {held_back} inspections held back until their printable is "
                 f"read; {given_up} shipped with the official result only (printable "
                 f"unreadable for {OC_PDF_GIVE_UP_DAYS}+ days)")
    poll['held_back'], poll['given_up'] = held_back, given_up
    if given_up and not poll.get('detail'):
        poll['detail'] = (f"{given_up} inspections shipped with the official result only "
                          f"(printable unreadable after {OC_PDF_GIVE_UP_TRIES} tries)")
    if limit:
        out = out[:limit]
    _OC_LAST_POLL.update(poll)
    log.info(f"Orange County: {len(out)} inspections since {since or 'the start of the sidecar'} "
             f"(sidecar {len(sidecar)}, pruned {poll['pruned']}, new {poll['new']}, "
             f"PDFs {poll['pdf_ok']}/{poll['pdf_tried']}, "
             f"without coordinates {sum(1 for r in out if not r.get('latitude'))})")
    return out


# ─── RICHARDSON (HealthTrak / Lotus Domino) ──────────────────────────────────

RICHARDSON_BASE = 'https://discovery.cor.gov/public/health/healthtrak.nsf/'
RICHARDSON_DELAY = 0.4


def _richardson_month_rows(session, year, month):
    """Fetch one month of Richardson scores. Returns [(name, address, date, score, detail_path)]."""
    r = session.get(RICHARDSON_BASE + 'getWebScoresByMonth',
                    params={'openagent': '', 'year': year, 'month': month},
                    timeout=30)
    if r.status_code != 200:
        return []
    rows = []
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', r.text, re.S):
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.S)
        if len(cells) < 4:
            continue
        clean = [html_lib.unescape(re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', c))).strip()
                 for c in cells[:4]]
        name, address, date_str, score_str = clean
        if not sane_inspection_date(date_str) or not score_str.isdigit():
            continue
        m = re.search(r'href="([^"]+\?OpenDocument)"', tr)
        rows.append((name, address, date_str, int(score_str), m.group(1) if m else None))
    return rows


def _richardson_parse_report(html):
    """
    Parse violations from a Richardson HealthTrak inspection report.
    Violated checklist items carry a demerit value in the first cell
    (e.g. "-3") and inspector remarks in the third cell. TX form weights:
    3 points = priority, 2 = priority foundation, 1 = core.
    """
    violations = []
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S):
        cells = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)
        if len(cells) < 3:
            continue
        clean = [html_lib.unescape(re.sub(r'\s+', ' ', re.sub(r'<br\s*/?>', ' | ', c)))
                 for c in cells[:3]]
        clean = [re.sub(r'<[^>]+>', ' ', c).strip() for c in clean]
        demerit_m = re.match(r'^-?(\d+)$', clean[0])
        if not demerit_m:
            continue
        item_title = re.sub(r'^\d+\.\s*', '', clean[1]).strip()
        remarks = re.sub(r'(\s*\|\s*)+', ' | ', clean[2]).strip(' |')
        desc = f"{item_title}: {remarks}" if remarks else item_title
        if len(desc) < 5:
            continue
        demerits = int(demerit_m.group(1))
        if demerits >= 3:
            sev = 'priority'
        elif demerits == 2:
            sev = 'priority_foundation'
        else:
            sev = 'core'
        cat, _, _ = classify_violation(desc)
        violations.append((cat, sev, desc[:500]))
    return violations


def fetch_richardson(since_date=None, until_date=None, limit=None, fetch_violations=True):
    """
    Fetch Richardson, TX inspections from the city's HealthTrak system
    (Lotus Domino app on discovery.cor.gov — a different host from the
    bot-blocked cor.net site). Monthly score tables (~100 inspections/month,
    0-100 scale) link to full inspection reports, which are parsed for
    violations with demerit-based severity.
    """
    log.info(f"Fetching Richardson data (since={since_date}, limit={limit})")
    session = requests.Session()
    session.headers.update({'User-Agent': CHROME_UA})

    start = datetime.strptime(str(since_date)[:10], '%Y-%m-%d') if since_date \
        else datetime(datetime.now().year, 1, 1)
    end = datetime.now()
    if until_date:
        end = min(end, datetime.strptime(str(until_date)[:10], '%Y-%m-%d'))

    months = []
    cursor = datetime(start.year, start.month, 1)
    while cursor <= end:
        months.append((cursor.year, cursor.month))
        cursor = datetime(cursor.year + (cursor.month == 12),
                          (cursor.month % 12) + 1, 1)

    results = []
    for year, month in months:
        if _past_deadline():
            log.warning(f"Richardson: fetch deadline reached — stopping at "
                        f"{year}-{month:02d} with {len(results)} inspections")
            break
        rows = _richardson_month_rows(session, year, month)
        log.info(f"  Richardson {year}-{month:02d}: {len(rows)} inspections")
        for name, address, date_str, score, detail_path in rows:
            if since_date and date_str < str(since_date)[:10]:
                continue
            violations = []
            if fetch_violations and detail_path:
                try:
                    dr = session.get('https://discovery.cor.gov' + detail_path, timeout=30)
                    if dr.status_code == 200:
                        violations = _richardson_parse_report(dr.text)
                except requests.RequestException as e:
                    log.debug(f"  Richardson detail failed: {e}")
                time.sleep(RICHARDSON_DELAY)
            _, pv, pfv, cv = calc_risk_score(violations)
            results.append({
                'name': name.title(),
                'address': address.title(),
                'city': 'Richardson',
                'state': 'TX',
                'zip': '',
                'latitude': None,
                'longitude': None,
                'inspection_date': date_str,
                'original_score': score,
                'risk_score': max(0, min(100, score)),
                'priority_violations': pv,
                'priority_foundation_violations': pfv,
                'core_violations': cv,
                'total_violations': len(violations),
                'violations': [[c, s, d] for c, s, d in violations],
                'inspection_type': '',
                'results': '',
                'source': 'Richardson Health Dept',
                'source_url': 'https://discovery.cor.gov/public/health/healthtrak.nsf/WebScoresByDay.html',
                'source_id': (detail_path or '').split('/')[-1].split('?')[0] or f"rich_{name}_{date_str}",
                'metro': 'DFW',
            })
            if limit and len(results) >= limit:
                break
        if limit and len(results) >= limit:
            break
        time.sleep(RICHARDSON_DELAY)

    log.info(f"Richardson: {len(results)} inspections")
    return results


# ─── ARLINGTON (ArcGIS Open Data) ────────────────────────────────────────────

ARLINGTON_LAYER_URL = ('https://gis2.arlingtontx.gov/agsext2/rest/services/'
                       'OpenData/OD_Community/MapServer/7')


def fetch_arlington(since_date=None, limit=None):
    """
    Fetch Arlington, TX food establishment inspections from the city's ArcGIS
    Open Data layer (one row per establishment with its most recent scored
    inspection; 0-100 scale, higher is better; updated weekdays). No violation
    text is published, so records carry scores/grades only.
    """
    log.info(f"Fetching Arlington data (since={since_date}, limit={limit})")
    results = []
    offset = 0
    page_size = 2000  # layer maxRecordCount
    while True:
        params = {
            'where': '1=1',
            'outFields': '*',
            'outSR': '4326',
            'orderByFields': 'OBJECTID',
            'resultOffset': offset,
            'resultRecordCount': page_size,
            'f': 'json',
        }
        r = requests.get(ARLINGTON_LAYER_URL + '/query', params=params,
                         headers={'User-Agent': CHROME_UA}, timeout=60)
        r.raise_for_status()
        data = r.json()
        features = data.get('features', [])
        if not features:
            break
        for feat in features:
            a = feat.get('attributes', {})
            geo = feat.get('geometry', {})
            name = (a.get('FacilityName') or '').strip()
            if not name:
                continue
            # Parse M/D/YYYY inspection date
            raw_date = (a.get('InspectionDateText') or '').strip()
            date_str = None
            if raw_date:
                try:
                    date_str = datetime.strptime(raw_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            date_str = sane_inspection_date(date_str)
            if not date_str:
                continue
            if since_date and date_str < str(since_date)[:10]:
                continue
            # Score is a string; blank for non-scored follow-ups (skip those)
            try:
                score = int(str(a.get('InspectionScore', '')).strip())
            except ValueError:
                continue
            results.append({
                'name': name.title(),
                'address': (a.get('PropertyAddress') or '').strip().title(),
                'city': 'Arlington',
                'state': 'TX',
                'zip': '',
                'latitude': geo.get('y'),
                'longitude': geo.get('x'),
                'inspection_date': date_str,
                'original_score': score,
                'risk_score': max(0, min(100, score)),
                'priority_violations': 0,
                'priority_foundation_violations': 0,
                'core_violations': 0,
                'total_violations': 0,
                'violations': [],
                'inspection_type': a.get('Inspection', ''),
                'results': a.get('Attempt', ''),
                'source': 'Arlington Open Data',
                'source_url': 'https://opendata.arlingtontx.gov/datasets/arlingtontx::food-establishments/about',
                'source_id': f"arlington_{a.get('OBJECTID', '')}",
                'metro': 'DFW',
            })
            if limit and len(results) >= limit:
                break
        if (limit and len(results) >= limit) or len(features) < page_size:
            break
        offset += page_size
        time.sleep(0.5)
    log.info(f"Arlington: {len(results)} scored food inspections")
    return results


# ─── 2026-11 EXPANSION: NEW SOURCES ─────────────────────────────────────────
# Each section below is one fetch group of the November 2026 expansion
# (Georgia, Louisiana, Maricopa County, Harris County, the Hedgerow portals,
# Williamson County and the uncovered DFW cities). They follow the API-fetcher
# contract (see fetch_wake / fetch_omaha): fetch_<x>(since_date=None, limit=None)
# returns per-inspection dicts; main() runs each in the pool under its group
# name and geocodes the records the source published without coordinates.
# A fetcher that cannot read its whole window raises or reports the group
# partial (_set_group_status) — it never returns a silently truncated list.

# ─── GEORGIA + LOUISIANA (Tyler "publicportals" JSON APIs) ──────────────────
# Georgia DPH (statewide food service, ga.healthinspections.us/stateofgeorgia)
# and Louisiana LDH (statewide retail food, la.healthinspections.us) run the
# same Tyler Technologies "publicportals" ColdFusion JSON API — a different
# product from the search.cfm portals Houston/San Antonio/DC scrape:
#   search/{json}/{page}      facilities whose LAST inspection falls in the
#                             filter window; the json's values are base64, the
#                             `keyword` key must always be present (Louisiana
#                             answers 500 without it); 5 rows per page, page
#                             numbers from 0, an empty list [] ends the list.
#                             Rows are sorted last-inspection-date DESC, then
#                             numeric facility id ASC (checked per day below).
#   inspectionsData/{id}      every inspection of one facility, newest first,
#                             violations inline; the id is the base64 string
#                             exactly as the list returns it.
#   facilities/{page}         the unfiltered list in the same order (page 0 =
#                             the newest inspection the portal holds).
# Neither portal publishes coordinates (main() geocodes); both hold about
# three years of history (from Nov 2023). Probed 2026-11-02 (NOTES.md in the
# probe captures); verified live again while building this on 2026-11-03.
#
# Fetch shape (shared, `_tp_fetch`): one single-day search per day from the
# lookback start to today, oldest first, each paged to its [] terminator and
# checked against the ordering invariant (every row dated that day, ids
# strictly ascending and distinct); a day whose pages shifted under the run
# is re-read, and a day that cannot be read completely RAISES — the group
# then records an error and ships nothing rather than a partial window. The
# date filter is on the facility's last inspection, so every listed facility
# costs one inspectionsData call (oldest-listed first, TP_WORKERS workers)
# and ships every inspection dated from TP_REREAD_MARGIN_DAYS before the
# window start (a dropped facility is re-listed under its newest day only;
# the margin recovers the older in-window inspections behind it), deduped by
# inspection id. A facility whose history could not be read drops its
# inspections and holds the source's freshness at the day it was listed
# under (key = its numeric id), so the next lookback re-reads it; deadline
# cuts hold without counting and report the group 'skipped'; an inspection
# whose entry is inconsistent or unreadable holds instead of shipping clean;
# TP_MASS_FAILURE_MIN unreadable histories make the run red, more than half
# abandon it. since_date=None (test mode) is one open window from the
# portal's history floor, newest first, stopped by `limit`. Measured
# 2026-11-03 on a 15-day window: Georgia
# 2,215 facilities -> 1,882 inspections, 2,685 requests, 9 min wall; Louisiana
# 1,434 facilities (LDH posts ~7 days late, so ~8 days of data) -> 1,392
# inspections, 3,127 requests, 66 min wall, 47 of them on the 1,084 printable
# notices (see LA_PRINTABLE_*). A 9-day weekly window costs about the same.

TP_PAGE_SIZE = 5           # fixed by the server (the SPA's infinite scroll)
TP_DELAY = 0.25            # pause after every list page / detail call
TP_WORKERS = 2             # inspectionsData and printable workers
TP_RETRIES = 4             # attempts per request (429/5xx/timeouts/resets)
TP_BACKOFF = 10            # seconds × attempt between retries
TP_TIMEOUT = 90
TP_DAY_REREADS = 2         # re-reads of a day whose pages shifted under the run
TP_MAX_DAY_PAGES = 2000    # runaway guard for one day's list (10,000 rows)
TP_MASS_FAILURE_MIN = 20   # unreadable facility histories in one run that make the run red (more than half: raise)
TP_REREAD_MARGIN_DAYS = 14 # a read facility also ships its inspections this many days before the window start
TP_LISTING_MARGIN_DAYS = int(os.environ.get('TP_LISTING_MARGIN_DAYS', '0'))   # widen every window start (late publication)
TP_HEADERS = {
    'User-Agent': CHROME_UA,
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
}
_TP_STATS = {}             # group -> {'requests', 'seconds'} for the run log
_TP_STATS_LOCK = threading.Lock()
# mapAddress is 'STREET \r\nCITY, ST ZIP[-4]': the LAST line is the city line
# and every earlier line belongs to the street. Georgia has three-line rows
# ('261 S LIBERTY ST \r\nPO BOX 125 \r\nWAYNESBORO, GA 30830', 'STREET\r\n
# SUITE 4\r\nCITY, ST ZIP'); splitting on the first newline put the middle
# line into the city ('Po Box 125 Waynesboro') and so into the restaurant id.
# A PO Box line is a mailing address, not the premises: dropped from the
# street unless it is all there is. Louisiana variants seen on 6 of 1,434
# live rows (2026-11-03): one line with no city ('402 N CARROLLTON AVE LA
# 70119 ', the city is in the 'City:' column) and a two-line address with a
# corrupted zip ('PAULINA, LA 7.076').
_TP_ADDR_CITY_RE = re.compile(r'^\s*(.+?),\s*([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\s*$')
_TP_ADDR_CITY_NO_ZIP_RE = re.compile(r'^\s*(.+?),\s*([A-Za-z]{2})\b')
_TP_ADDR_ONE_LINE_RE = re.compile(r'^\s*(.*?\S)\s+([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\s*$')
_TP_PO_BOX_RE = re.compile(r'^\s*P\.?\s*O\.?\s*BOX\b', re.I)
# Inspectors' work e-mail addresses and phone numbers turn up in a few notes
# ('send an update to first.last@dph.ga.gov', 8 of 3,045 live lines); they
# add nothing to the finding and name a person, so they never ship.
_TP_EMAIL_RE = re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')
_TP_PHONE_RE = re.compile(r'(?<![\d-])(?:\(\d{3}\)\s*|\d{3}[-.\s])\d{3}[-.\s]\d{4}(?![\d-])')
_TP_DATE_RE = re.compile(r'(\d{2})-(\d{2})-(\d{4})\s*$')

GA_API = 'https://ga.healthinspections.us/stateofgeorgia/API/index.cfm/'
GA_PORTAL = 'https://ga.healthinspections.us/stateofgeorgia/'
GA_GROUP = 'Georgia'
GA_SOURCE = 'Georgia DPH'
GA_HISTORY_START = '2023-11-01'     # earliest inspection seen: 2023-11-06
GA_PERMIT_TYPE = 'Food Service'     # the other permit types are Tourist Accommodation and Swimming Pool
# Inspection purposes that are not a fixed food establishment: 'Temporary'
# is a festival/event booth permit (the Renaissance Festival's stands all
# share the fairground's address).
GA_SKIP_PURPOSES = frozenset({'Temporary'})
# Gwinnett, Newton and Rockdale are GNR Health (the MHD 'gwinnett' slug) and
# never appear in the statewide portal (0 rows for each county filter,
# verified 2026-11-02 and 2026-11-03); each run re-checks the three county
# filters over its window and drops anything they list.
GA_EXCLUDED_COUNTIES = ('GWINNETT', 'NEWTON', 'ROCKDALE')
# The county is not in a facility row; the only county-level signal is the
# county Environmental Health office phone in 'For More Information Call:'.
# One number per county (verified 2026-11-03 on 20 rows of each: 111 distinct
# numbers across the state's 156 portal counties). Digits only -> county, for
# the nine counties that get the 'Atlanta' metro chip.
GA_ATLANTA_EH_PHONES = {
    '7705207500': 'Fulton', '4045087900': 'DeKalb', '7704357815': 'Cobb',
    '6786107469': 'Clayton', '7709207311': 'Douglas', '7704790444': 'Cherokee',
    '7702886190': 'Henry', '9432098057': 'Fayette', '7707816909': 'Forsyth',
}
GA_METRO = 'Atlanta'
# Severity is the citation's designation suffix: '(p)' priority, '(pf)'
# priority foundation, '(c)' core, combined '(p, c)' / '(pf, c)' / '(p, pf, c)'
# (the highest wins). 11 of 340 probed citations were null and one ended in
# '©' instead of '(c)': those fall back to the item's Points (9 = a risk
# factor / intervention item = priority, 4 = priority foundation, 3/2/1 =
# good retail practices = core).
_GA_DESIGNATION_RE = re.compile(r'\(\s*((?:pf?|c)(?:\s*,\s*(?:pf?|c))*)\s*\)\s*$', re.I)
_GA_ITEM_CODE_RE = re.compile(r'^\s*\d{1,2}(?:-\d)?[A-Za-z]?\s*-\s*')
# Inspector Notes carry the observation plus, often, the corrective action
# ('CA:', 'C/A:', '(CA)', 'CA-', 'Rule:', 'Code:', 'RCA:') and the rule text
# pasted after its citation ('511-6-1.05(7)(b) - Food Contact Surfaces ...
# (P, C) (b) Equipment ...'). Only the observation ships: rule text names
# pests and sewage as a matter of course and must never read as evidence of
# them. Rule sentences pasted with no marker at all ("Employees shall not
# ...", 17% of 3,045 live lines on 2026-11-03) are left in place: none of
# them fired the pest/sewage regex, and cutting at the sentence would have
# emptied observations that put the rule first.
_GA_NOTES_PREFIX_RE = re.compile(r'^\s*Inspector Notes:\s*', re.I)
_GA_CODE_REF_RE = re.compile(r'Violation of Code:\s*\[[^\]]*\]\s*', re.I)
_GA_RULE_RE = re.compile(r'\b511-6-1[-.]+\s*\d')      # '511-6-1.05(7)(b)' and '511-6-1-.04'
_GA_CA_RE = re.compile(r'(?:\bCorrective Actions?\b\s*[:\-]?|\(\s*CA\s*\)|\bC/A\b\s*[:\-]?'
                       r'|\b[Cc][Aa]\s*[:\-–]|\bCA\s+(?=[A-Z])|\b(?:Rule|Code|RCA)\s*:)')
_GA_OBS_RE = re.compile(r'\b(?:Observed|Observation|Observations)\b\s*:?|\bCOS\b|\bCorrected on site\b', re.I)
_GA_FSIM_RE = re.compile(r'[?&]fsimID=(\d+)')      # the printable's inspection id, equal to inspectionId

LA_API = 'https://la.healthinspections.us/API/index.cfm/'
LA_PORTAL = 'https://la.healthinspections.us/'
LA_GROUP = 'Louisiana'
LA_SOURCE = 'Louisiana LDH'
LA_HISTORY_START = '2023-11-07'     # the portal's oldest inspection
# LDH publishes no score: the two statutory categories score the inspection
# (calc_risk_score: 100 - 5 per Critical - 1 per Non-Critical item). Jev
# treats the label as source_tiers (jev_reader.SOURCE_TIER_SCORE_SOURCES).
LA_SEVERITY = {'Critical Items': 'priority', 'Non-Critical Items': 'core'}
LA_CLEAN_MARKER = ['None']          # a clean inspection is exactly {'0': ['None']}
LA_METRO_PARISHES = {
    'Orleans': 'New Orleans', 'Jefferson': 'New Orleans',
    'St. Tammany': 'New Orleans', 'St. Bernard': 'New Orleans',
    'East Baton Rouge': 'Baton Rouge', 'West Baton Rouge': 'Baton Rouge',
    'Ascension': 'Baton Rouge', 'Livingston': 'Baton Rouge',
}
# The tail of 'Permit Name: <establishment> <tail>' is free text the
# sanitarian typed, not a permit-type vocabulary: sampled live 2026-11-03 over
# 250 recently inspected facilities / 2,995 inspections it had 221 distinct
# values ('Restaurant', '(RESTAURANT)', '- KITCHEN', 'Dietary', 'fast food',
# '225', a casino's "Grotto" and "Bar 46", street names in parentheses) and
# 378 inspections had no tail at all. LDH lists every retail food permit
# together (restaurants with school cafeterias, day cares, hospitals, nursing
# homes, jails), and Georgia's single 'Food Service' permit type covers its
# school, hospice and senior-center kitchens too, so the scope rule for both
# states is a word-boundary keyword denylist over the facility name (plus
# Louisiana's permit name): an institutional kitchen the public cannot walk
# into is skipped; everything else — restaurants, bars, groceries,
# markets, delis, bakeries, caterers, casino and hotel kitchens, mobile units
# (LDH lists them at their base address) — ships. 'cafeteria' alone is not a
# trigger (Piccadilly Cafeteria is a restaurant chain); 'Summer Feeding'
# inspections (school-site meal programs) are skipped by purpose.
TP_INSTITUTION_RE = re.compile(
    r"\b(?:(?<!old )schools?|elementary|middle school|high school|junior high|academy|kindergarten|"
    r"pre-?k|preschool|head start|day ?cares?|child ?care|learning cent(?:er|re)|nursery|dietary|"
    r"hospitals?|nursing|rehabilitation|rehab|health ?care|medical center|assisted living|"
    r"retirement|senior cent(?:er|re)|council on aging|feeding site|group home|community home|"
    r"boys'? home|girls'? home|behavioral|hospice|residential|"
    r"jails?|prisons?|correctional|detention|penitentiary|food pantry|shelter|summer feeding)\b",
    re.I)
LA_SKIP_PURPOSES = frozenset({'Summer Feeding'})
# The inspectionsData `violations` dict is NOT the list of findings: it holds
# one entry per (category, item, tag) — an item cited twice under two code
# sections appears once, an item whose one finding is both Corrected and
# Repeat appears twice (measured 2026-11-03 over 810 inspections: 244
# Corrected+Repeat pairs, 37 None+Corrected, never two entries with the same
# tag). The findings are the rows of the printable "Retail Food Notice of
# Violations" (HTML, ~5 KB): a CRITICAL ITEMS table and a NON-CRITICAL ITEMS
# table (each printed only when it has rows), one row per cited violation
# with the category, the LAC Title 51 code and '<item#> - <code> -
# <narrative> [Repeat|Corrected]'. Cuccio's Food Mart's 10-12-2026 routine:
# 14 entries for 13 items, 19 rows — risk 65 (F) from the notice, 71 (C)
# from the items. So when the notice is read, the violations, their tiers and
# the score are rebuilt from its rows; the JSON's distinct items (tag
# duplicates merged) are the titles-only fallback, which can only undercount.
# One notice per shipped inspection with items, oldest first, within the
# fetch deadline and LA_PRINTABLE_MAX_MINUTES (the pass runs inside the API
# pool, and every minute of it delays the sequential portal groups): an
# inspection the budget or the deadline left unread ships on its items and
# holds the window without counting, so the next run completes it; one whose
# notice failed (503s, 404, a page that is not a notice, another date) ships
# on its items for good and never holds — one flaky notice must not re-read a
# week of notices. LA_PRINTABLE_MAX_PER_RUN (0 = titles only, for backfills)
# skips without holding.
LA_PRINTABLE_MAX_PER_RUN = int(os.environ.get('LA_PRINTABLE_MAX_PER_RUN', '6000'))
LA_PRINTABLE_MAX_MINUTES = float(os.environ.get('LA_PRINTABLE_MAX_MINUTES', '90'))
# The /paper/ endpoint is rate-limited like a small token bucket (measured
# 2026-11-03: about four requests, then paired instant IIS 503s with
# Retry-After: 3 that clear within five seconds; the JSON API is not), so the
# printables run on ONE worker, spaced, and a 503 waits the hinted seconds
# before one of LA_PRINTABLE_RETRIES retries. ~2.3 s per printable measured.
LA_PRINTABLE_WORKERS = 1
LA_PRINTABLE_DELAY = 1.2
LA_PRINTABLE_RETRIES = 3
LA_PRINTABLE_RETRY_WAIT = 3.5      # when the 503 carries no usable Retry-After
_LA_ITEM_RE = re.compile(r'^\s*(\d+)\s*-\s*(.*?)\s*$', re.S)
_LA_PRINTABLE_ROW_RE = re.compile(
    r'<tr>\s*<td>(?P<cat>[^<]*)</td>\s*<td>(?P<code>[^<]*)</td>\s*<td>(?P<desc>.*?)</td>\s*</tr>', re.S)
# '<item#> - <LAC code> - <narrative>'; one live form (an older school-kitchen
# notice, 2026-10-26) prints '<item#> - <narrative>' with no code segment.
_LA_PRINTABLE_DESC_RE = re.compile(r'^(\d+)\s*-\s*(?:\d[\d.]*\s*-\s*)?(.*)$', re.S)
_LA_PRINTABLE_DATE_RE = re.compile(r'Date<br>\s*(\d{2})/(\d{2})/(\d{4})')
_LA_TAG_RE = re.compile(r'\s*\[(?:Repeat|Corrected)\]\s*', re.I)
_LA_REPEAT_TAG_RE = re.compile(r'\[Repeat\]', re.I)
_LA_CRIT_HEADER_RE = re.compile(r'(?<!NON-)CRITICAL ITEMS:')
_LA_NONCRIT_HEADER_RE = re.compile(r'NON-CRITICAL ITEMS:')
_LA_PRINTABLE_ID_RE = re.compile(r'[?&]inspectionID=(\d+)')   # the notice's inspection id, equal to inspectionId


def _tp_count(group, seconds):
    with _TP_STATS_LOCK:
        s = _TP_STATS.setdefault(group, {'requests': 0, 'seconds': 0.0})
        s['requests'] += 1
        s['seconds'] += seconds


def _tp_search_url(api, filters, page):
    """The SPA's search URL: a JSON object of base64-encoded filter values
    (keyword always present), '/' replaced by '%2F' inside the JSON, then
    percent-encoded as one path segment, followed by the 0-based page."""
    obj = {k: base64.b64encode(str(v).encode('utf-8')).decode('ascii') for k, v in filters.items()}
    obj.setdefault('keyword', '')
    js = json.dumps(obj, separators=(',', ':')).replace('/', '%2F')
    return f'{api}search/{quote(js, safe="%")}/{page}'


def _tp_get_json(session, url, group, referer):
    """GET one API resource; the parsed JSON, or None once TP_RETRIES attempts
    are exhausted. 429/5xx, timeouts, resets and non-JSON bodies are retried
    with backoff (never past the fetch deadline); any other status is final."""
    headers = dict(TP_HEADERS, Referer=referer)
    what = url.rsplit('index.cfm/', 1)[-1][:90]      # the resource, for the log
    last = ''
    for attempt in range(1, TP_RETRIES + 1):
        t0 = time.time()
        try:
            r = session.get(url, headers=headers, timeout=TP_TIMEOUT)
            _tp_count(group, time.time() - t0)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    last = f'non-JSON body {r.text[:60]!r}'
            elif r.status_code in (429, 500, 502, 503, 504):
                last = f'HTTP {r.status_code}'
            else:
                log.warning(f"  {group}: HTTP {r.status_code} on {what}")
                return None
        except requests.RequestException as e:
            _tp_count(group, time.time() - t0)
            last = str(e)[:80]
        if attempt == TP_RETRIES or _past_deadline():
            break
        wait = TP_BACKOFF * attempt
        log.warning(f"  {group}: {last} on {what} — retry in {wait}s ({attempt}/{TP_RETRIES - 1})")
        time.sleep(wait)
    log.warning(f"  {group}: giving up on {what}: {last}")
    return None


def _tp_institutional(name, permit_name=''):
    """True for an institutional kitchen by name (see TP_INSTITUTION_RE)."""
    return bool(TP_INSTITUTION_RE.search(f'{name} {permit_name}'))


def _tp_title(s):
    """Collapse whitespace; title-case an ALL-CAPS value keeping possessives."""
    s = ' '.join(str(s or '').split())
    if s.isupper():
        s = re.sub(r"'S\b", "'s", s.title())
    return s


def _tp_strip_contacts(text):
    """Replace e-mail addresses and phone numbers in free text with a token."""
    return _TP_PHONE_RE.sub('[phone]', _TP_EMAIL_RE.sub('[email]', str(text or '')))


def _tp_parse_address(map_address, state, city_hint=''):
    """'STREET \\r\\n[MORE \\r\\n]CITY, ST ZIP[-4]' -> (street, city, zip5), or
    None. The LAST line is the city line and the earlier lines are the street
    (PO Box lines dropped). A one-line 'STREET ST ZIP' takes its city from
    `city_hint` (the row's 'City:' column); a city line with an unreadable zip
    ships with zip ''. The state must match: a facility is never placed in
    another."""
    lines = [ln.strip() for ln in str(map_address or '').replace('\r', '\n').split('\n') if ln.strip()]
    if not lines:
        return None
    if len(lines) == 1:
        m = _TP_ADDR_ONE_LINE_RE.match(lines[0])
        if not m:
            return None
        street, city, st, zip5 = m.group(1), city_hint, m.group(2), m.group(3)
    else:
        m = _TP_ADDR_CITY_RE.match(lines[-1])
        if m:
            city, st, zip5 = m.group(1), m.group(2), m.group(3)
        else:
            m = _TP_ADDR_CITY_NO_ZIP_RE.match(lines[-1])
            if not m:
                return None
            city, st, zip5 = m.group(1), m.group(2), ''
        parts = [ln for ln in lines[:-1] if not _TP_PO_BOX_RE.match(ln)] or lines[:-1]
        street = ' '.join(parts)
    if st.upper() != state:
        return None
    street, city = _tp_title(street).strip(' ,'), _tp_title(city)
    if not street or not city:
        return None
    return street, city, zip5


def _tp_date(value):
    """'Date: MM-DD-YYYY', 'Last Inspection Date: MM-DD-YYYY' or 'MM-DD-YYYY'
    -> 'YYYY-MM-DD' (via sane_inspection_date), or None."""
    m = _TP_DATE_RE.search(str(value or ''))
    if not m:
        return None
    return sane_inspection_date(f'{m.group(3)}-{m.group(1)}-{m.group(2)}')


def _tp_column(record, prefix):
    """The value of the record column that starts with `prefix` ('' if none)."""
    for v in (record.get('columns') or {}).values():
        if isinstance(v, str) and v.startswith(prefix):
            return v[len(prefix):].strip()
    return ''


def _tp_row_day(row):
    return _tp_date(_tp_column(row, 'Last Inspection Date:'))


def _tp_facility_id(b64_id):
    """The decimal facility id behind the list's base64 id, or None."""
    try:
        n = base64.b64decode(str(b64_id or ''), validate=True).decode('ascii')
        return int(n) if n.isdigit() else None
    except (ValueError, UnicodeDecodeError):
        return None


def _tp_order_problem(rows, day=None):
    """'' when `rows` obey the portal's ordering (dated `day` when given, else
    dates non-increasing; numeric ids strictly ascending within a date and
    distinct), otherwise a one-line description of the shift."""
    seen, prev = set(), None
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            return f'row {i} is not an object'
        fid = _tp_facility_id(row.get('id'))
        d = _tp_row_day(row)
        if fid is None or not d:
            return f'row {i} has no readable id/date'
        if day and d != day:
            return f'row {i} is dated {d}, not {day}'
        if fid in seen:
            return f'facility {fid} listed twice'
        seen.add(fid)
        if prev is not None:
            if d > prev[0]:
                return f'row {i} ({d}) is newer than the row before it ({prev[0]})'
            if d == prev[0] and fid <= prev[1]:
                return f'row {i} (id {fid}) is out of id order after {prev[1]}'
        prev = (d, fid)
    return ''


class _TpDeadlineCut(RuntimeError):
    """The fetch deadline passed in the middle of a day's facility list."""


def _tp_day_rows(session, cfg, day):
    """Every facility whose last inspection is dated `day` (all pages of one
    single-day search). Raises RuntimeError when a page cannot be read or the
    day's pages kept shifting under the run, _TpDeadlineCut when the deadline
    passed while a page was being retried."""
    group = cfg['group']
    problem = ''
    for attempt in range(TP_DAY_REREADS + 1):
        rows, page = [], 0
        while True:
            data = _tp_get_json(session, _tp_search_url(cfg['api'], cfg['day_filters'](day), page),
                                group, cfg['portal'])
            if not isinstance(data, list):
                if _past_deadline():
                    raise _TpDeadlineCut(f'{group}: fetch deadline reached while listing {day}')
                raise RuntimeError(f'{group}: search page {page} for {day} could not be read')
            if not data:
                break
            rows.extend(data)
            page += 1
            if page > TP_MAX_DAY_PAGES:
                raise RuntimeError(f'{group}: the {day} list exceeds {TP_MAX_DAY_PAGES} pages')
            time.sleep(TP_DELAY)
        problem = _tp_order_problem(rows, day)
        if not problem:
            return rows
        log.warning(f"  {group}: the {day} list shifted under the run ({problem}) — re-reading")
    raise RuntimeError(f'{group}: the {day} facility list kept shifting ({problem})')


def _tp_open_rows(session, cfg, start, max_rows):
    """The open window from `start` to today, newest first (since_date=None):
    pages until [] or `max_rows` facilities. Returns (rows, listed_to_the_end)."""
    group = cfg['group']
    rows, page = [], 0
    while True:
        if _past_deadline():
            raise RuntimeError(f'{group}: fetch deadline reached while listing the history')
        data = _tp_get_json(session, _tp_search_url(cfg['api'], cfg['open_filters'](start), page),
                            group, cfg['portal'])
        if not isinstance(data, list):
            raise RuntimeError(f'{group}: history list page {page} could not be read')
        if not data:
            return rows, True
        rows.extend(data)
        page += 1
        if max_rows and len(rows) >= max_rows:
            return rows, False
        if page % 200 == 0:
            log.info(f"  {group}: {len(rows)} facilities listed so far...")
        time.sleep(TP_DELAY)


def _tp_newest_listed(session, cfg):
    """The newest 'Last Inspection Date' the portal holds (its unfiltered list
    is sorted newest first), '' if the probe fails — for the lag note only."""
    data = _tp_get_json(session, f"{cfg['api']}facilities/0", cfg['group'], cfg['portal'])
    if not isinstance(data, list):
        return ''
    return max((_tp_row_day(r) or '' for r in data if isinstance(r, dict)), default='')


def _tp_abs_url(portal, path):
    """Absolute, percent-encoded URL for a printablePath relative to the portal."""
    from urllib.parse import urljoin
    path = str(path or '').strip()
    if not path:
        return portal
    return requests.utils.requote_uri(urljoin(portal, path))


def _tp_fetch(cfg, since_date, limit):
    """Shared Tyler publicportals fetch (see the section comment). `cfg` holds
    api, portal, group, source, history_start, day_filters(day) and
    open_filters(start) -> search filter dicts, build(row, hist, ship_from,
    day) -> (records, hold_dates) for one facility, and optionally
    excluded_ids(session, since) and enrich(session, records)."""
    group, source = cfg['group'], cfg['source']
    with _TP_STATS_LOCK:
        _TP_STATS[group] = {'requests': 0, 'seconds': 0.0}
    started = time.time()
    since = str(since_date)[:10] if since_date else None
    if since and TP_LISTING_MARGIN_DAYS > 0:
        since = (datetime.strptime(since, '%Y-%m-%d') - timedelta(days=TP_LISTING_MARGIN_DAYS)).strftime('%Y-%m-%d')
    start = since or cfg['history_start']
    # A facility read this run ships its inspections back to `ship_from`. The
    # list filter is on the facility's LAST inspection, so a facility a failed
    # or cut run dropped is re-listed under its newest day only, and without
    # the margin its older in-window inspections (the routine behind a
    # follow-up) would fall before the re-read window and be lost. Re-shipping
    # is idempotent downstream; the margin covers any gap inside a normal
    # window and a 14-day gap inside a longer catch-up window.
    ship_from = ((datetime.strptime(since, '%Y-%m-%d') - timedelta(days=TP_REREAD_MARGIN_DAYS)).strftime('%Y-%m-%d')
                 if since else None)
    today = datetime.now().strftime('%Y-%m-%d')
    log.info(f"Fetching {group} data (since={since_date}, limit={limit})")
    local = threading.local()

    def session():
        if not hasattr(local, 's'):
            local.s = requests.Session()
        return local.s

    # 1. List the facilities with a last inspection in the window.
    facilities = {}            # base64 id -> (row, day listed under)
    complete = True            # the whole window was listed and nothing was cut
    if since:
        first = datetime.strptime(start, '%Y-%m-%d')
        days = [(first + timedelta(days=i)).strftime('%Y-%m-%d')
                for i in range((datetime.strptime(today, '%Y-%m-%d') - first).days + 1)]
        if not days:
            days = [start]
        for i, day in enumerate(days):
            day_rows = None
            if not _past_deadline():
                try:
                    day_rows = _tp_day_rows(session(), cfg, day)
                except _TpDeadlineCut:
                    day_rows = None
            if day_rows is None:
                # Hold at the first unread day so the next lookback starts there.
                _hold_freshness(source, day, counted=False)
                _set_group_status(group, 'skipped',
                                  f'fetch deadline reached; {len(days) - i} days not listed')
                complete = False
                break
            for row in day_rows:
                facilities.setdefault(row['id'], (row, day))
            if limit and len(facilities) >= limit:
                complete = False
                break
    else:
        rows, ended = _tp_open_rows(session(), cfg, start, limit)
        for row in rows:
            if isinstance(row, dict) and row.get('id'):
                facilities.setdefault(row['id'], (row, _tp_row_day(row)))
        complete = ended
    log.info(f"  {group}: {len(facilities)} facilities listed since {start}"
             f"{'' if complete else ' (bounded read)'}")

    # 2. Facilities another source already covers (Georgia's GNR counties).
    if facilities and cfg.get('excluded_ids'):
        excluded = cfg['excluded_ids'](session(), start)
        dropped = [fid for fid in facilities if fid in excluded]
        for fid in dropped:
            facilities.pop(fid)
        if dropped:
            log.warning(f"  {group}: dropped {len(dropped)} facilities listed under an excluded county")

    # 3. Histories, oldest-listed first so a deadline cut drops the newest tail.
    ordered = sorted(facilities, key=lambda fid: (facilities[fid][1] or '', _tp_facility_id(fid) or 0))

    def work(fid):
        if _past_deadline():
            return fid, 'deadline'
        data = _tp_get_json(session(), f"{cfg['api']}inspectionsData/{fid}", group, cfg['portal'])
        time.sleep(TP_DELAY)
        if isinstance(data, list):
            return fid, data
        return fid, 'deadline' if _past_deadline() else 'failed'

    histories, done = {}, 0
    if ordered:
        with ThreadPoolExecutor(max_workers=TP_WORKERS) as pool:
            for future in as_completed([pool.submit(work, fid) for fid in ordered]):
                fid, hist = future.result()
                histories[fid] = hist
                done += 1
                if done % 250 == 0:
                    log.info(f"  {group}: {done}/{len(ordered)} facility histories read")

    # A portal that stops answering fails facility after facility; those are
    # not per-record failures (which the per-record give-up would write off
    # after three runs), so TP_MASS_FAILURE_MIN unreadable histories make the
    # run red, and more than half of them abandon it: nothing ships, nothing
    # is counted against any record, the window is re-read next run.
    tried = sum(1 for h in histories.values() if h != 'deadline')
    unreadable = sum(1 for h in histories.values() if h == 'failed')
    if unreadable >= TP_MASS_FAILURE_MIN and unreadable * 2 > tried:
        raise RuntimeError(f'{group}: {unreadable} of {tried} facility histories could not be read — '
                           f'the portal is not answering; nothing shipped')
    if unreadable >= TP_MASS_FAILURE_MIN:
        _set_group_status(group, 'error', f'{unreadable} of {tried} facility histories could not be read '
                                          f'(dropped and held for the next run)')

    # 4. Records: every inspection dated from `ship_from`, deduped by
    #    inspection id; a facility that could not be read drops and holds the
    #    day it was listed under (counted only when it was tried).
    records, seen, failed, cut = [], set(), 0, 0
    for fid in ordered:
        row, day = facilities[fid]
        hist = histories.get(fid, 'failed')
        key = _tp_facility_id(fid) or fid
        if hist == 'deadline':
            cut += 1
            _hold_freshness(source, day, counted=False, key=key)
            continue
        if hist == 'failed':
            failed += 1
            _hold_freshness(source, day, counted=True, key=key)
            continue
        try:
            recs, holds = cfg['build'](row, hist, ship_from, day)
        except Exception as e:  # noqa: BLE001 - one bad facility must not sink the state
            log.warning(f"  {group}: facility {key} could not be parsed ({e}); dropped and held")
            _hold_freshness(source, day, counted=True, key=key)
            failed += 1
            continue
        for d in holds:
            _hold_freshness(source, d, counted=True, key=key)
        for r in recs:
            if r['source_id'] in seen:
                continue
            seen.add(r['source_id'])
            records.append(r)
    if cut:
        complete = False
        _set_group_status(group, 'skipped', f'fetch deadline reached; {cut} facilities not read')
    if failed or cut:
        log.warning(f"  {group}: {failed} facilities could not be read{f', {cut} not read (deadline)' if cut else ''}"
                    f" — their inspections are dropped and held for the next run")

    # 5. Newest first, worst first within a day; `limit` keeps the newest and
    #    holds at the newest dropped date (a bounded window run re-reads them).
    def newest_first(rs):
        rs.sort(key=lambda r: (r['inspection_date'], -r['risk_score'], r['total_violations'],
                               r['source_id']), reverse=True)
    newest_first(records)
    if limit and len(records) > limit:
        _hold_freshness(source, records[limit]['inspection_date'], counted=False)
        records = records[:limit]
        complete = False

    # 6. Per-source enrichment (Louisiana's notices rebuild text, tiers and
    #    score), then the same-day order again.
    if records and cfg.get('enrich'):
        cfg['enrich'](session, records)
        newest_first(records)

    # 7. The lag note only after a complete read (never from a bounded one).
    if complete:
        newest = max([d for _, d in facilities.values() if d] + [_tp_newest_listed(session(), cfg)],
                     default='')
        _note_source_lag(group, newest)
    stats = _TP_STATS.get(group) or {}
    log.info(f"{group}: {len(records)} inspections from {len(facilities)} facilities — "
             f"{stats.get('requests', 0)} requests, {stats.get('seconds', 0):.0f}s in requests, "
             f"{time.time() - started:.0f}s wall")
    return records


# ── Georgia ──────────────────────────────────────────────────────────────────

def _ga_severity(citation, points):
    """Tier from the citation's designation, else from the item's Points."""
    m = _GA_DESIGNATION_RE.search(str(citation or ''))
    if m:
        tags = {t.strip().lower() for t in m.group(1).split(',')}
        if 'p' in tags:
            return 'priority'
        if 'pf' in tags:
            return 'priority_foundation'
        return 'core'
    pm = re.search(r'\d+', str(points or ''))
    pts = int(pm.group()) if pm else 0
    if pts >= 9:
        return 'priority'
    if pts >= 4:
        return 'priority_foundation'
    return 'core'


def _ga_item_title(item):
    """'6-1B - proper hot holding temperatures' -> 'Proper hot holding temperatures'."""
    s = ' '.join(_GA_ITEM_CODE_RE.sub('', str(item or ''), count=1).split())
    return s[:1].upper() + s[1:]


def _ga_observation(notes):
    """The inspector's observation from an 'Inspector Notes:' value, without
    the corrective-action segments and the pasted rule text."""
    s = _GA_NOTES_PREFIX_RE.sub('', str(notes or ''))
    s = _GA_CODE_REF_RE.sub('', s)
    s = ' '.join(s.split())
    m = _GA_RULE_RE.search(s)
    if m:
        s = s[:m.start()]
    kept, pos = [], 0
    while True:
        m = _GA_CA_RE.search(s, pos)
        if not m:
            kept.append(s[pos:])
            break
        kept.append(s[pos:m.start()])
        nxt = _GA_OBS_RE.search(s, m.end())
        if not nxt:
            break
        pos = nxt.start()
    return _tp_strip_contacts(' '.join(''.join(kept).split())).strip(' .;:-,')


def _ga_violation(line):
    """One inspectionsData violation (a 6-string list) -> [category, severity,
    text], or None for an unreadable entry."""
    if not isinstance(line, list) or len(line) < 2:
        return None
    title = _ga_item_title(line[0])
    obs = _ga_observation(line[5] if len(line) > 5 else '')
    text = f'{title}: {obs}' if obs else title
    if len(line) > 4 and str(line[4] or '').strip().lower() == 'repeat: yes':
        text += ' (repeat violation)'
    if not text.strip():
        return None
    text = text[:500]
    cat, _, _ = classify_violation(text)
    return [cat, _ga_severity(line[1], line[2] if len(line) > 2 else ''), text]


def _ga_build(row, hist, ship_from, day):
    """Records for one Georgia facility: every scored Food Service inspection
    dated on or after `ship_from` (all when None). Returns (records,
    hold_dates). Held, never shipped: an inspection whose violations are not
    readable (not a dict, an unreadable entry, or no items under a score
    below 100 — the score is 100 minus the OUT items' points, so deductions
    with no items are an incomplete entry), one whose inspectionId is missing
    or disagrees with its printable's fsimID (the portal once served a hybrid
    of two inspections: a new date and score under an old id with no items),
    and a listed day missing from the history."""
    name = _tp_title(row.get('name'))
    parsed = _tp_parse_address(row.get('mapAddress'), 'GA')
    if not name or not parsed:
        log.warning(f"  Georgia: facility {row.get('id')} has no usable name/address; skipped")
        return [], []
    if _tp_column(row, 'Permit Type:') not in ('', GA_PERMIT_TYPE) or _tp_institutional(name):
        return [], []           # not Food Service, or an institutional kitchen by name
    street, city, zip5 = parsed
    phone = re.sub(r'\D', '', _tp_column(row, 'For More Information Call:'))
    metro = GA_METRO if phone in GA_ATLANTA_EH_PHONES else ''
    out, holds, listed_seen = [], [], False
    for insp in hist:
        if not isinstance(insp, dict):
            continue
        date = _tp_date(_tp_column(insp, 'Date:'))
        if not date:
            continue
        if date == day:
            listed_seen = True
        if ship_from and date < ship_from:
            continue
        purpose = _tp_column(insp, 'Inspection Purpose:')
        if purpose in GA_SKIP_PURPOSES:
            continue
        try:
            score = int(float(_tp_column(insp, 'Score:')))
        except (TypeError, ValueError):
            score = 0
        if score <= 0:
            continue                    # an unscored visit has no grade basis
        insp_id = insp.get('inspectionId')
        fsim = _GA_FSIM_RE.search(str(insp.get('printablePath') or ''))
        if (not isinstance(insp_id, int) or isinstance(insp_id, bool)
                or (fsim and int(fsim.group(1)) != insp_id)):
            holds.append(date)          # an inconsistent entry: never ship it under a wrong id
            continue
        viol = insp.get('violations')
        if not isinstance(viol, dict) or (not viol and score < 100):
            holds.append(date)          # unreadable violations, or deductions with no items: never clean
            continue
        violations = []
        for k in sorted(viol, key=lambda x: int(x) if str(x).isdigit() else 0):
            v = _ga_violation(viol[k])
            if v is None:
                violations = None       # one unreadable entry: the inspection is not read
                break
            violations.append(v)
        if violations is None:
            holds.append(date)
            continue
        pv = sum(1 for v in violations if v[1] == 'priority')
        pfv = sum(1 for v in violations if v[1] == 'priority_foundation')
        out.append({
            'name': name,
            'address': street,
            'city': city,
            'state': 'GA',
            'zip': zip5,
            'latitude': None,
            'longitude': None,
            'inspection_date': date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': len(violations) - pv - pfv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': purpose,
            'results': '',
            'source': GA_SOURCE,
            'source_url': _tp_abs_url(GA_PORTAL, insp.get('printablePath')),
            'source_id': f'ga_{insp_id}',
            'metro': metro,
        })
    if day and not listed_seen:
        holds.append(day)               # the inspection the list promised is not in the history
    return out, holds


def _ga_excluded_ids(session, start):
    """Facility ids the three GNR counties list for the window (normally none).
    Raises when a page cannot be read: the check fails closed."""
    excluded = set()
    for county in GA_EXCLUDED_COUNTIES:
        page = 0
        while True:
            data = _tp_get_json(session, _tp_search_url(
                GA_API, {'county': county, 'permitType': GA_PERMIT_TYPE, 'from': start}, page),
                GA_GROUP, GA_PORTAL)
            if not isinstance(data, list):
                raise RuntimeError(f'Georgia: the {county} county check could not be read')
            if not data:
                break
            excluded.update(r.get('id') for r in data if isinstance(r, dict))
            page += 1
            if page > TP_MAX_DAY_PAGES:
                break
            time.sleep(TP_DELAY)
        if page:
            log.warning(f"  Georgia: the statewide portal lists {county} facilities "
                        f"(GNR Health, covered by the MHD 'gwinnett' slug) — dropping them")
    return excluded


def fetch_georgia(since_date=None, limit=None):
    """
    Fetch Georgia DPH food service inspections statewide (156 counties:
    Atlanta's Fulton/DeKalb/Cobb and the rest of the state; Gwinnett, Newton
    and Rockdale are GNR Health on the MHD 'gwinnett' slug and are not in this
    portal). Official 0-100 score (A 90+ / B 80-89 / C 70-79 / U below, the
    same cutoffs DineScores uses) as original_score and risk_score; severity
    from the state's (p)/(pf)/(c) citation designations; violation text = the
    form item's title plus the inspector's observation. 'Temporary' (event
    booth) inspections are skipped. metro 'Atlanta' for the nine core counties
    (from the row's county EH phone), '' elsewhere. No coordinates: main()
    geocodes. See the section comment for the window/hold/deadline rules.
    """
    cfg = {
        'api': GA_API, 'portal': GA_PORTAL, 'group': GA_GROUP, 'source': GA_SOURCE,
        'history_start': GA_HISTORY_START,
        # inclusive on both ends
        'day_filters': lambda day: {'permitType': GA_PERMIT_TYPE, 'from': day, 'to': day},
        'open_filters': lambda start: {'permitType': GA_PERMIT_TYPE, 'from': start},
        'build': _ga_build,
        'excluded_ids': _ga_excluded_ids,
    }
    return _tp_fetch(cfg, since_date, limit)


# ── Louisiana ────────────────────────────────────────────────────────────────



def _la_item(title):
    """'23 - food safety certification' -> (23, 'Food safety certification')."""
    m = _LA_ITEM_RE.match(str(title or ''))
    if not m:
        s = ' '.join(str(title or '').split())
        return None, s[:1].upper() + s[1:]
    text = ' '.join(m.group(2).split())
    return int(m.group(1)), text[:1].upper() + text[1:]


def _la_parse_printable(html_text):
    """The Notice of Violations -> {'date': 'YYYY-MM-DD', 'rows': [(severity,
    item number, category, narrative, repeat), ...]} in form order, the
    severity from the table the row sits in (CRITICAL ITEMS / NON-CRITICAL
    ITEMS; a table is printed only when it has rows), or None when the page
    is not a notice or has neither table."""
    text = str(html_text or '')
    if 'Notice of Violations' not in text:
        return None
    dm = _LA_PRINTABLE_DATE_RE.search(text)
    if not dm:
        return None
    crit = _LA_CRIT_HEADER_RE.search(text)
    nonc = _LA_NONCRIT_HEADER_RE.search(text)
    if not crit and not nonc:
        return None
    rows = []
    for m in _LA_PRINTABLE_ROW_RE.finditer(text):
        desc = ' '.join(html_lib.unescape(re.sub(r'<[^>]+>', ' ', m.group('desc'))).split())
        dm2 = _LA_PRINTABLE_DESC_RE.match(desc)
        if not dm2:
            continue
        repeat = bool(_LA_REPEAT_TAG_RE.search(dm2.group(2)))
        narrative = _LA_TAG_RE.sub(' ', dm2.group(2)).replace('| ', ', ').replace('|', ',')
        narrative = _tp_strip_contacts(' '.join(narrative.split()).strip(' -'))
        in_critical = bool(crit) and m.start() > crit.start() and (not nonc or m.start() < nonc.start())
        category = ' '.join(html_lib.unescape(m.group('cat')).split())
        rows.append(('priority' if in_critical else 'core', int(dm2.group(1)), category, narrative, repeat))
    return {'date': f'{dm.group(3)}-{dm.group(1)}-{dm.group(2)}', 'rows': rows}


def _la_build_factory(aux):
    """_la_build with a side table `aux`: source_id -> (printable url,
    {item number: title}) for the notice pass."""
    def build(row, hist, ship_from, day):
        name = _tp_title(row.get('name'))
        parsed = _tp_parse_address(row.get('mapAddress'), 'LA', _tp_column(row, 'City:'))
        if not name or not parsed:
            log.warning(f"  Louisiana: facility {row.get('id')} has no usable name/address; skipped")
            return [], []
        street, city, zip5 = parsed
        parish = _tp_column(row, 'Parish:')
        metro = LA_METRO_PARISHES.get(parish, '')
        out, holds, listed_seen = [], [], False
        for insp in hist:
            if not isinstance(insp, dict):
                continue
            date = _tp_date((insp.get('columns') or {}).get('0'))
            if not date:
                continue
            if date == day:
                listed_seen = True
            if ship_from and date < ship_from:
                continue
            purpose = _tp_column(insp, 'Inspection Purpose:')
            if purpose in LA_SKIP_PURPOSES or _tp_institutional(row.get('name'), _tp_column(insp, 'Permit Name:')):
                continue
            insp_id = insp.get('inspectionId')
            path = str(insp.get('printablePath') or '')
            pm = _LA_PRINTABLE_ID_RE.search(path)
            if (not isinstance(insp_id, int) or isinstance(insp_id, bool)
                    or (pm and int(pm.group(1)) != insp_id)):
                holds.append(date)      # an inconsistent entry: never ship it under a wrong id
                continue
            viol = insp.get('violations')
            if not isinstance(viol, dict) or not viol:
                holds.append(date)      # missing, unreadable or EMPTY: LDH's clean marker is explicit
                continue
            lines = [viol[k] for k in sorted(viol, key=lambda x: int(x) if str(x).isdigit() else 0)]
            if all(line == LA_CLEAN_MARKER for line in lines):
                lines = []
            items, bad = [], False       # [severity, item number, title, repeat]; tag duplicates merged
            for line in lines:
                if not isinstance(line, list) or len(line) < 2 or not line[1]:
                    bad = True
                    break
                sev = LA_SEVERITY.get(str(line[0]).strip())
                if sev is None:
                    log.warning(f"  Louisiana: unknown category label {line[0]!r} on inspection {insp_id}; held")
                    bad = True
                    break
                number, title = _la_item(line[1])
                repeat = len(line) > 2 and str(line[2] or '').strip(' -').lower() == 'repeat'
                if items and items[-1][:3] == [sev, number, title]:
                    items[-1][3] = items[-1][3] or repeat   # one item, several tags: one finding at least
                else:
                    items.append([sev, number, title, repeat])
            if bad:
                holds.append(date)      # a malformed entry: never ship it as read
                continue
            violations = []
            for sev, number, title, repeat in items:
                text = (title + (' (repeat violation)' if repeat else ''))[:500]
                violations.append([classify_violation(text)[0], sev, text])
            risk, pv, pfv, cv = calc_risk_score(violations)
            sid = f'lald_{insp_id}'
            url = _tp_abs_url(LA_PORTAL, path)
            aux[sid] = (url if path else '', {number: title for _, number, title, _ in items})
            out.append({
                'name': name,
                'address': street,
                'city': city,
                'state': 'LA',
                'zip': zip5,
                'latitude': None,
                'longitude': None,
                'inspection_date': date,
                'original_score': None,     # LDH publishes no score
                'risk_score': risk,
                'priority_violations': pv,
                'priority_foundation_violations': pfv,
                'core_violations': cv,
                'total_violations': len(violations),
                'violations': violations,
                'inspection_type': purpose,
                'results': '',
                'source': LA_SOURCE,
                'source_url': url,
                'source_id': sid,
                'metro': metro,
            })
        if day and not listed_seen:
            holds.append(day)
        return out, holds
    return build


def _la_enrich_factory(aux):
    """Notice pass: GET each shipped inspection's printable notice (oldest
    first, LA_PRINTABLE_WORKERS workers, within the fetch deadline,
    LA_PRINTABLE_MAX_MINUTES and LA_PRINTABLE_MAX_PER_RUN) and rebuild its
    violations, counts and score from the notice's rows: the tier from the
    table, the JSON's item title plus the inspector's narrative. An
    inspection the budget or the deadline left unread ships on its items and
    holds the window (uncounted) so the next run completes it; a notice that
    failed keeps the items for good."""
    def enrich(session, records):
        started = time.time()
        todo = [r for r in records if r['violations'] and aux.get(r['source_id'], ('', {}))[0]]
        todo.sort(key=lambda r: (r['inspection_date'], r['source_id']))
        skipped = max(0, len(todo) - LA_PRINTABLE_MAX_PER_RUN)
        todo = todo[:LA_PRINTABLE_MAX_PER_RUN]
        if not todo:
            return
        log.info(f"  Louisiana: reading {len(todo)} printable notices"
                 f"{f' ({skipped} beyond the per-run cap keep their item titles)' if skipped else ''}...")
        counts = defaultdict(int)

        def work(rec):
            url = aux[rec['source_id']][0]
            headers = {'User-Agent': CHROME_UA, 'Referer': LA_PORTAL}
            for attempt in range(1, LA_PRINTABLE_RETRIES + 1):
                if _past_deadline():
                    return rec, 'deadline'
                if time.time() - started > LA_PRINTABLE_MAX_MINUTES * 60:
                    return rec, 'budget'
                t0 = time.time()
                try:
                    r = session().get(url, headers=headers, timeout=TP_TIMEOUT)
                    _tp_count(LA_GROUP, time.time() - t0)
                    time.sleep(LA_PRINTABLE_DELAY)
                    if r.status_code == 200:
                        return rec, _la_parse_printable(r.text) or 'unparsed'
                    if r.status_code not in (429, 500, 502, 503, 504):
                        return rec, 'failed'
                    try:
                        wait = float(r.headers.get('Retry-After', ''))
                    except (TypeError, ValueError):
                        wait = LA_PRINTABLE_RETRY_WAIT
                    wait = min(max(wait, 1.0), 10.0) + 0.5
                except requests.RequestException:
                    _tp_count(LA_GROUP, time.time() - t0)
                    wait = LA_PRINTABLE_RETRY_WAIT
                if attempt < LA_PRINTABLE_RETRIES:
                    time.sleep(wait)
            return rec, 'failed'

        with ThreadPoolExecutor(max_workers=LA_PRINTABLE_WORKERS) as pool:
            for future in as_completed([pool.submit(work, r) for r in todo]):
                rec, parsed = future.result()
                if parsed in ('deadline', 'budget'):
                    counts[parsed] += 1
                    _hold_freshness(LA_SOURCE, rec['inspection_date'], counted=False, key=rec['source_id'])
                    continue
                if not isinstance(parsed, dict):
                    counts[parsed] += 1
                    continue
                if parsed['date'] != rec['inspection_date']:
                    counts['date mismatch'] += 1
                    continue
                if not parsed['rows']:
                    counts['no rows'] += 1      # items in the JSON, none on the notice: not read
                    continue
                titles = aux[rec['source_id']][1]
                violations = []
                for sev, number, category, narrative, repeat in parsed['rows']:
                    title = titles.get(number) or _tp_title(category) or f'Item {number}'
                    text = f'{title}: {narrative}' if narrative else title
                    if repeat:
                        text += ' (repeat violation)'
                    text = text[:500]
                    violations.append([classify_violation(text)[0], sev, text])
                risk, pv, pfv, cv = calc_risk_score(violations)
                rec.update({'violations': violations, 'risk_score': risk, 'priority_violations': pv,
                            'priority_foundation_violations': pfv, 'core_violations': cv,
                            'total_violations': len(violations)})
                counts['read'] += 1
        unread = counts.get('deadline', 0) + counts.get('budget', 0)
        if unread:
            why = ' and '.join(w for w, n in (('the fetch deadline', counts.get('deadline')),
                                              (f'the {LA_PRINTABLE_MAX_MINUTES:g}-minute notice budget',
                                               counts.get('budget'))) if n)
            log.warning(f"  Louisiana: {unread} notices not read ({why}) — those inspections ship on their "
                        f"item titles and are held for the next run")
        log.info("  Louisiana: notices " + ', '.join(f"{n} {why}" for why, n in sorted(counts.items())))
    return enrich


def fetch_louisiana(since_date=None, limit=None):
    """
    Fetch Louisiana LDH retail food inspections statewide (all 64 parishes;
    metro chips 'New Orleans' for Orleans/Jefferson/St. Tammany/St. Bernard
    and 'Baton Rouge' for East/West Baton Rouge, Ascension and Livingston).
    LDH publishes no score or grade: original_score is None and risk_score =
    100 - 5 per Critical finding - 1 per Non-Critical finding (calc_risk_score
    over the state's own two categories). The findings are the rows of the
    inspection's printable Notice of Violations, each the state's item title
    plus the inspector's narrative, tier from the table it sits in; until the
    notice is read (budget, deadline) or when it cannot be (a failed fetch, a
    page that is not a notice), the inspection ships on the JSON's distinct
    items (tag duplicates merged) with their titles — see the LA_PRINTABLE_*
    comment. A clean inspection ({'0': ['None']}) ships with no violations
    and risk 100; an empty dict is held, not clean. Institutional permit
    types (schools, day cares, hospitals, nursing homes, jails...) are
    skipped (TP_INSTITUTION_RE). No coordinates: main() geocodes. See the
    section comment for the window/hold/deadline rules; the date bounds here
    are strictly exclusive, so one day is from=day-1, to=day+1.
    """
    aux = {}

    def day_filters(day):
        d = datetime.strptime(day, '%Y-%m-%d')
        return {'keyword': '', 'from': (d - timedelta(days=1)).strftime('%Y-%m-%d'),
                'to': (d + timedelta(days=1)).strftime('%Y-%m-%d')}

    def open_filters(start):
        d = datetime.strptime(start, '%Y-%m-%d')
        return {'keyword': '', 'from': (d - timedelta(days=1)).strftime('%Y-%m-%d')}

    cfg = {
        'api': LA_API, 'portal': LA_PORTAL, 'group': LA_GROUP, 'source': LA_SOURCE,
        'history_start': LA_HISTORY_START,
        'day_filters': day_filters,
        'open_filters': open_filters,
        'build': _la_build_factory(aux),
        'enrich': _la_enrich_factory(aux),
    }
    return _tp_fetch(cfg, since_date, limit)


# ─── PHOENIX / MARICOPA COUNTY, AZ (envapp.maricopa.gov + county ArcGIS) ────
# Maricopa County Environmental Services (MCESD) publishes food inspections
# only through its ASP.NET "Restaurant Ratings Tool" at envapp.maricopa.gov.
# There is no open-data copy and no server-side date filter on any
# captcha-free endpoint (the one date-filtered report, /Report/WeeklyReport,
# sits behind reCAPTCHA and is never used), so the fetcher is a three-phase
# scrape with structural completeness checks, measured 2026-11-02/03:
#   1. INVENTORY  POST /Search/Results once per 5-digit zip (the county
#      ArcGIS layer's zip list unioned with the static 85001-85399 range):
#      every permit in a zip comes back in one JSON body. Every response
#      must be 200 with a JSON 'data' list or the group raises before
#      anything ships (a missed zip would silently hide its permits). Only
#      MCESD_FOOD_PERMIT_TYPES are kept (mobile units have no fixed address).
#   2. LISTING    GET /Permit/PermitResults/{permitId} for every food permit:
#      a server-rendered table of the permit's inspections for a rolling
#      ~3 years (date, purpose, grade, status, priority count, detail link).
#      A page counts as read only when it is 200 AND carries the
#      'N Inspections Retrieved' marker AND the table has exactly N rows;
#      anything else is unread (one re-fetch, then the window is held with
#      the permit as key and the group goes red). Rows dated inside the
#      window whose (restaurant, date) is not already stored go on.
#   3. DETAIL     GET /Inspection/{permitId}/{inspectionId}, oldest first:
#      the header labels plus div#Violations (read to its OWN closing tag,
#      nested divs counted), whose 'Comments:' text states each line's tier
#      in two generations of wording (2024+ ' - Priority:' / ' - Priority
#      Foundation:' / ' - Core:' with the observation after 'Addl Notes:';
#      legacy ', P:' / ', Pf:' / ', C:' with it after '>>>'). One county
#      item can cite several sections, each with its own tier and notes:
#      every citation is its own violation line. An entry is five kinds of
#      paragraph (number, description, the 'Comments:' label, the comments,
#      a 'Repeat Violation' flag): any other text inside it, a missing
#      number or description, no comments paragraph at all, text in
#      the block that is not an entry, a block headed 'Violations' with no
#      entry, or a tier the parser cannot read makes the page UNREAD
#      (dropped and held), never clean. A whitespace-only div#Violations is
#      a real clean inspection, and an entry whose comments paragraph is
#      PRESENT and empty (a re-inspection's re-checked item) adds nothing:
#      both verified on live pages. A comments paragraph the parser cannot
#      find is a layout change, never an empty entry: that rule once made a
#      renamed paragraph class ship every PF/Core-only page as a clean 100.
#      A header Priority tally above the Priority citations read is UNREAD
#      too (a citation the parser missed); one below them is only logged:
#      the county does not tally a suspension's or a re-inspection's
#      citations, nor one corrected on site.
# The county publishes no numeric score: original_score is None and
# risk_score = 100 - 5P - 2PF - C from the source-stated tiers (the label is
# in jev_reader's SOURCE_TIER_SCORE_SOURCES / SOURCE_TIER_SOURCES, so Jev
# never moves a tier). The county's letter grade (A/B/C/D, 'Cutting Edge
# A+', 'Not Participating', 'R' = re-inspection, blank = advisory) goes to
# results. Coordinates come from the county's public ArcGIS facilities layer
# joined on LicenseNumber == permitId (rows duplicated ~4x with identical
# geometry); permits without a point ship with None and main() geocodes.
# Identity (make_restaurant_id) comes from the search row's businessName and
# its 'STREET CITY AZ ZIP' concat split with MCESD_MULTIWORD_CITIES — never
# from the ArcGIS City field, which carries typos (AONVDALE, PHEONIX). The
# concat's quirks, each seen on live rows 2026-11-03 (21,724 food rows): the
# state twice with the city's comma ('1825 N central AVE Phoenix, AZ AZ
# 85004'), a nine-digit zip without its hyphen ('850222301') or a cut +4
# ('85043-22'), a comma inside the street ('176 Wickenburg, Way'), a highway
# number ('AZ-74') and two rows with no city at all. The splitter normalises
# the first two (city and address are id inputs, so 'Az' as a city or a zip
# inside the street would strand a restaurant's history once fixed) and
# leaves the street as printed otherwise.
# No run-to-run inventory count comparison: a fetcher has no sane place to
# persist last week's count (the freshness file belongs to _record_group),
# so MCESD_MIN_FOOD_PERMITS catches a collapsed inventory instead, on every
# scan the record limit did not stop (the weekly run passes limit=50,000,
# which is not a test mode: only a scan that stopped AT its limit is
# exempt, and only such a scan leaves no source-lag note). Detail pages the
# portal answered but that gave no inspection ('unread': a page the parser
# rejects; 'unavailable': the app's 'Inspection Not Available') are dropped
# and held like any other failure and, from MCESD_UNREAD_ERROR_MIN of them
# in one run, make the group 'error' with every id in the log: a parser
# blind spot is a steady percentage, not a transient, and a held record is
# given up after FRESHNESS_HOLD_GIVE_UP_RUNS runs. An unread PERMIT page
# holds the window at the requested start (freshness has no per-permit
# scope): free in weekly mode, whose window already reaches back
# MCESD_POSTING_LAG_DAYS, but a --since-date backfill WITH --freshness-file
# would pin the next weeklies' window at the backfill's start for up to
# three runs (re-detailing only rows not in SKIP_KNOWN_INSPECTIONS), so a
# backfill runs without the freshness file.

MCESD_BASE = 'https://envapp.maricopa.gov'
MCESD_SEARCH_URL = MCESD_BASE + '/Search/Results'
MCESD_PERMIT_URL = MCESD_BASE + '/Permit/PermitResults/{permit}'
MCESD_INSPECTION_URL = MCESD_BASE + '/Inspection/{permit}/{inspection}'
MCESD_ARCGIS_LAYER = ('https://services.arcgis.com/ykpntM6e3tHvzKRJ/arcgis/rest/services/'
                      'Restaurant_(External)/FeatureServer/0')
MCESD_SOURCE = 'Maricopa County Environmental Services'
MCESD_GROUP = 'Maricopa County'
MCESD_METRO = 'Phoenix'
# Food establishments that belong on a restaurant map. Of the 25,828 permits
# on 2026-11-02: Eating & Drinking 16,039, Retail Food 2,791, Micro Market
# 908, Meat Market 738, Food Catering 649, Bakery 598 (= 21,723). Dropped:
# every 'Mobile Food Annual' type (MF- permits, no fixed location), Daycare
# Food Service (child care), Food Production / Commissary / Food Jobber /
# Refrigerated Warehouse / Ice Manufacturing / Bottled Water and Beverage
# Plant (not open to the public), Boarding Home, Food Bank, Damaged Food
# and Adventure Food Service. The permit type is the only scope signal the
# source gives: school, hospital and workplace cafeterias are permitted as
# Eating & Drinking and ship, as they do from every other source (a name
# filter would be guesswork); pools, body art and child care are separate
# county programs that never appear in this search.
MCESD_FOOD_PERMIT_TYPES = frozenset({
    'Food Establishment - Eating & Drinking',
    'Food Establishment - Retail Food',
    'Food Establishment - Micro Market',
    'Food Establishment - Meat Market',
    'Food Establishment - Food Catering',
    'Food Establishment - Bakery',
})
MCESD_ZIP_RANGE = (85001, 85399)     # every Maricopa County zip; unioned with ArcGIS's list
MCESD_MIN_FOOD_PERMITS = 15000       # 21,723 on 2026-11-02; fewer means the inventory broke
MCESD_HISTORY_DAYS = 3 * 366         # the portal's rolling window (oldest row seen: 1,096 days)
# The county posts inspections days to weeks after they happen: on 2026-11-03
# the newest row across all 21,722 permits was dated 2026-10-29. The weekly
# lookback starts the day before the newest SHIPPED date, so a window that
# began there would skip every inspection dated earlier but published later.
# The fetcher therefore reaches back at least this many days on every run
# (inspections already stored are skipped, not re-read, via SKIP_KNOWN_INSPECTIONS).
MCESD_POSTING_LAG_DAYS = int(os.environ.get('MCESD_POSTING_LAG_DAYS', '21'))
MCESD_SEARCH_WORKERS = 2
MCESD_SEARCH_RATE = 4.0              # zip searches per second, all workers together
MCESD_LISTING_WORKERS = int(os.environ.get('MCESD_LISTING_WORKERS', '4'))
MCESD_LISTING_RATE = float(os.environ.get('MCESD_LISTING_RATE', '7'))   # permit pages/s in total
MCESD_DETAIL_WORKERS = int(os.environ.get('MCESD_DETAIL_WORKERS', '2'))
MCESD_DETAIL_DELAY = float(os.environ.get('MCESD_DETAIL_DELAY', '0.5'))  # per worker, after a page
MCESD_TIMEOUT = 60
MCESD_ATTEMPTS = 3                   # tries per request on 429/5xx/transport errors
MCESD_BACKOFF = (3, 8)               # seconds between those tries
MCESD_BREAKER_TRIP = 25              # consecutive unreadable pages = the portal is blocking
MCESD_UNREAD_ERROR_MIN = 5           # detail pages answered but giving no inspection (parser-rejected
                                     # or 'Not Available') in one run before the group is 'error'
MCESD_ARCGIS_CHUNK = 60              # permit ids per ArcGIS 'LicenseNumber IN (...)' GET: 75 ids
                                     # (1,739-char URL) answered 200, 100 ids (2,239 chars) 404
# Visits that are not inspections: advisory visits (consultations, no grade,
# zero violations on all 45 seen 2026-10-20..29), plan reviews and the
# 'Food AMC Trainings' visit. Shipping them as 100-point inspections would
# mask a bad routine in the weighted score. Matched on the row's purpose; an
# unknown new purpose ships (and shows in the log) rather than vanishing.
MCESD_SKIP_PURPOSE_RE = re.compile(r'advisory|training|review', re.I)
# A lone last word that is a street suffix means the county left the city
# out of the concat ('1155 W OCOTILLO RD AZ 85240'): no city, not 'Rd'.
MCESD_STREET_SUFFIXES = frozenset({
    'RD', 'ST', 'AVE', 'DR', 'BLVD', 'PKWY', 'LN', 'WAY', 'PL', 'HWY', 'CIR', 'CT', 'TRL',
    'LOOP', 'FWY', 'EXPY', 'TER', 'RTE', 'PKY', 'BLV', 'AV', 'STE', 'UNIT', 'BLDG', 'SUITE',
})
# Multi-word place names the county prints in 'STREET CITY AZ ZIP'; a
# single-word city needs no entry (the last word before 'AZ' is the city).
# Longest first so 'SUN CITY WEST' wins over 'SUN CITY'.
MCESD_MULTIWORD_CITIES = tuple(sorted((
    'APACHE JUNCTION', 'CASA GRANDE', 'CAVE CREEK', 'EL MIRAGE', 'FORT MCDOWELL',
    'FOUNTAIN HILLS', 'GILA BEND', 'GOLD CANYON', 'LITCHFIELD PARK',
    'LUKE AIR FORCE BASE', 'LUKE AFB', 'NEW RIVER', 'PARADISE VALLEY', 'QUEEN CREEK',
    'RIO VERDE', 'SAN TAN VALLEY', 'SUN CITY WEST', 'SUN CITY', 'SUN LAKES',
    'TORTILLA FLAT',
), key=len, reverse=True))
# The tail is 'AZ 85004', 'AZ 85004-1234', a nine-digit 'AZ 850222301' or a
# cut 'AZ 85043-22' (all live); the first five digits are the zip.
MCESD_CONCAT_RE = re.compile(r'^(?P<body>.*?)\s+AZ\s+(?P<zip>\d{5})(?:-?\d{1,4})?\s*$', re.I)
MCESD_STATE_TAIL_RE = re.compile(r'(?:[\s,]+AZ)+$', re.I)     # 'Phoenix, AZ AZ 85004': the state twice
MCESD_RETRIEVED_RE = re.compile(r'(\d+)\s+Inspections?\s+Retrieved')
MCESD_TBODY_RE = re.compile(r'<tbody>(.*?)</tbody>', re.S)
MCESD_TR_RE = re.compile(r'<tr>(.*?)</tr>', re.S)
MCESD_TD_RE = re.compile(r'<td[^>]*>(.*?)</td>', re.S)
MCESD_LINK_RE = re.compile(r'href="/Inspection/([^/"]+)/([^/"]+)"')
MCESD_TAG_RE = re.compile(r'<[^>]+>')
MCESD_HEADER_RE = re.compile(
    r'<span class="fw-bold">([^<]+)</span><br\s*/?>\s*(?:<span>)?(.*?)(?:</span>)?\s*</div>', re.S)
MCESD_VIOLATIONS_OPEN_RE = re.compile(r'<div[^>]*\bid="Violations"[^>]*>')
MCESD_DIV_TAG_RE = re.compile(r'<(/?)div\b[^>]*>', re.I)
MCESD_VIOLATIONS_H2_RE = re.compile(r'<h2>\s*Violations\s*</h2>')
# A violation entry is five kinds of paragraph (every live page, 2026-11-03):
# number, description, the 'Comments:' label, the comments (MCESD_PARA_RE)
# and the 'Repeat Violation' flag. Outside the comments and the flags its
# visible text must be exactly the labels, number and title: any other text
# is a layout change the parser does not know, so the page is unread.
MCESD_FLAG_RE = re.compile(r'<p class="mb-0 text-danger">(.*?)</p>', re.S)
MCESD_VIOLATION_NUM_RE = re.compile(r'<span class="fw-bold">Violation #:</span>\s*([^<]*?)\s*</p>')
MCESD_DESC_RE = re.compile(r'<span class="fw-bold">Description:</span>(.*?)</p>', re.S)
MCESD_PARA_RE = re.compile(r'<p class=[\'"]mb-1[\'"]>(.*?)</p>', re.S)
MCESD_TIER_MARK_RE = re.compile(r'\s-\s(Priority Foundation|Priority|Core):|,\s*(Pf|P|C):\s', re.I)
MCESD_CODE_RE = re.compile(r'(?:PRIORITY VIOLATION-|Priority Foundation-|Core-)?\d-\d{3}\.\d+', re.I)
MCESD_CODE_REACH = 70                # chars from a citation's start to its tier marker, at most
MCESD_TIER = {'priority': 'priority', 'p': 'priority',
              'priority foundation': 'priority_foundation', 'pf': 'priority_foundation',
              'core': 'core', 'c': 'core'}
MCESD_UNAVAILABLE = 'alert-heading">Inspection Not Available'   # the app's page for an id it no longer serves
_MCESD_LAST_STATS = {}               # the last fetch_maricopa run's counters (for measurement)


class _McesdPacer:
    """Shared request pacing across worker threads: at most `rate` requests
    per second in total, each caller sleeping once for its slot (no spin)."""

    def __init__(self, rate):
        self.interval = 1.0 / float(rate) if rate and rate > 0 else 0.0
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self):
        if not self.interval:
            return
        with self.lock:
            now = time.time()
            slot = max(now, self.next_at)
            self.next_at = slot + self.interval
        if slot > now:
            time.sleep(slot - now)


class _McesdStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = defaultdict(int)

    def bump(self, key, n=1):
        with self.lock:
            self.counts[key] += n


def _mcesd_clean(text):
    """Collapse whitespace; title-case an ALL CAPS value (mixed case kept)."""
    s = ' '.join(str(text or '').split())
    return s.title() if s.isupper() else s


def _mcesd_text(fragment):
    """Visible text of an HTML fragment, whitespace collapsed."""
    return ' '.join(html_lib.unescape(MCESD_TAG_RE.sub(' ', fragment or '')).split())


def _mcesd_split_address(concat):
    """'12223 N CAVE CREEK RD PHOENIX AZ 85022' -> ('12223 N CAVE CREEK RD',
    'PHOENIX', '85022'). A concat without the 'AZ ZIP' tail is all street.
    A state printed twice and a comma after the city ('1825 N central AVE
    Phoenix, AZ AZ 85004') give the same street and city as the plain form:
    both are make_restaurant_id inputs."""
    s = ' '.join(str(concat or '').split())
    m = MCESD_CONCAT_RE.match(s)
    if not m:
        return s, '', ''
    zip5 = m.group('zip')
    body = MCESD_STATE_TAIL_RE.sub('', m.group('body').strip()).strip(' ,.')
    upper = body.upper()
    for city in MCESD_MULTIWORD_CITIES:
        if upper.endswith(' ' + city):
            return body[:-len(city)].strip(' ,.'), body[-len(city):], zip5
    parts = body.rsplit(' ', 1)
    if len(parts) == 2:
        last = parts[1].strip('.,')
        if not last or last.upper() in MCESD_STREET_SUFFIXES or last.strip('#').isdigit():
            return body, '', zip5            # the county left the city out
        return parts[0].strip(' ,.'), last, zip5
    return '', body, zip5                    # a lone word is the city; no street


def _mcesd_date(text):
    """'8/18/2026' -> '2026-08-18' (sane), else None."""
    try:
        return sane_inspection_date(datetime.strptime(str(text or '').strip(), '%m/%d/%Y').strftime('%Y-%m-%d'))
    except ValueError:
        return None


def _mcesd_results(grade, status):
    """The county's grade card text for `results`, plus a re-inspection flag."""
    g = ' '.join(str(grade or '').split())
    gl = g.lower()
    if gl in ('a', 'b', 'c', 'd'):
        out = f'Grade {g.upper()}'
    elif gl == 'cutting edge a+':
        out = 'Grade A+ (Cutting Edge)'
    elif gl == 'not participating':
        out = 'Not Participating'
    elif gl == 'r':
        out = 'Re-inspection'
    elif gl in ('', 'na', 'n/a'):
        out = ''
    else:
        out = g                         # an unexpected value stays visible, never drops the row
    st = ' '.join(str(status or '').split())
    if st.lower() == 're-inspection required':
        out = f'{out}; re-inspection required' if out else 'Re-inspection required'
    return out


def _mcesd_request(session, method, url, data=None, pacer=None, stats=None):
    """One portal request with retries and backoff on 429/5xx/transport
    errors, stopping at the fetch deadline. Returns the final response, or
    None when no attempt got an answer; the caller decides what a non-200
    means. A 400/404/410 is final at once (that id's own problem)."""
    headers = {'User-Agent': CHROME_UA,
               'Accept': 'text/html,application/json;q=0.9,*/*;q=0.8'}
    if method == 'POST':
        headers['X-Requested-With'] = 'XMLHttpRequest'
    last = None
    for attempt in range(MCESD_ATTEMPTS):
        if attempt and _past_deadline():
            break
        if pacer is not None:
            pacer.wait()
        if stats is not None:
            stats.bump('requests')
        try:
            if method == 'POST':
                last = session.post(url, data=data, headers=headers, timeout=MCESD_TIMEOUT)
            else:
                last = session.get(url, headers=headers, timeout=MCESD_TIMEOUT)
        except requests.RequestException as e:
            log.debug(f"  Maricopa: {method} {url}: {e}")
            last = None
        if last is not None and (last.status_code < 400 or last.status_code in (400, 404, 410)):
            return last
        if stats is not None:
            stats.bump('retries')
        if attempt < MCESD_ATTEMPTS - 1 and not _past_deadline():
            time.sleep(MCESD_BACKOFF[min(attempt, len(MCESD_BACKOFF) - 1)])
    return last


def _mcesd_zip_list():
    """5-digit zips to enumerate: the static county range unioned with the
    ArcGIS layer's distinct zips (stripped of +4), so a permit in an
    unexpected zip is still found. The static range alone if ArcGIS fails."""
    zips = {f'{z:05d}' for z in range(MCESD_ZIP_RANGE[0], MCESD_ZIP_RANGE[1] + 1)}
    try:
        r = requests.get(MCESD_ARCGIS_LAYER + '/query', params={
            'where': '1=1', 'outFields': 'Zip', 'returnGeometry': 'false',
            'returnDistinctValues': 'true', 'f': 'json'},
            headers={'User-Agent': CHROME_UA}, timeout=120)
        r.raise_for_status()
        for f in (r.json().get('features') or []):
            m = re.match(r'\s*(\d{5})', str((f.get('attributes') or {}).get('Zip') or ''))
            if m:
                zips.add(m.group(1))
    except Exception as e:  # noqa: BLE001 - the static range still covers the county
        log.warning(f"  Maricopa: ArcGIS zip list unavailable ({e}); using the static range only")
    return sorted(zips)


def _mcesd_search_zip(session, zip5, pacer, stats):
    """Every permit row in a zip. Raises when the search cannot be read
    completely: a missed zip must fail the group, not hide its permits."""
    r = _mcesd_request(session, 'POST', MCESD_SEARCH_URL,
                       data={'licenseSearchParams[Zip]': zip5}, pacer=pacer, stats=stats)
    if r is None or r.status_code != 200:
        raise RuntimeError(f"Maricopa inventory: zip {zip5} search answered "
                           f"{'nothing' if r is None else r.status_code}")
    try:
        body = r.json()
    except ValueError:
        raise RuntimeError(f"Maricopa inventory: zip {zip5} search returned non-JSON")
    rows = body.get('data') if isinstance(body, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError(f"Maricopa inventory: zip {zip5} search has no 'data' list")
    return rows


def _mcesd_permit(row):
    """A food permit dict from a search row, or None for anything that does
    not belong on the map (mobile units, non-food types, junk rows)."""
    pid = ' '.join(str(row.get('permitId') or '').split())
    ptype = ' '.join(str(row.get('permitType') or '').split())
    if not pid or ptype not in MCESD_FOOD_PERMIT_TYPES or pid.upper().startswith('MF'):
        return None
    street, city, zip5 = _mcesd_split_address(row.get('businessAddressConcat'))
    name = _mcesd_clean(row.get('businessName'))
    if not name or not street:
        return None
    return {'permit': pid, 'type': ptype, 'name': name, 'address': _mcesd_clean(street),
            'city': ' '.join(city.split()).title() if city else '', 'zip': zip5}


def _mcesd_inventory(session_for, zips, limit, stats):
    """(permitId -> permit dict over `zips` in deterministic order, bounded):
    `bounded` is True when the scan stopped because `limit` food permits were
    already known (test mode's 25) rather than at the last zip, so the
    caller knows whether the count may be checked against
    MCESD_MIN_FOOD_PERMITS; `limit` itself says nothing about the mode (the
    weekly run passes 50,000). Raises on a search that cannot be read;
    (None, False) when the deadline cut the scan."""
    pacer = _McesdPacer(MCESD_SEARCH_RATE)
    permits, rows_seen, scanned = {}, 0, 0
    other_types, junk = defaultdict(int), 0
    bounded = False
    batch = max(1, MCESD_SEARCH_WORKERS) * 4
    with ThreadPoolExecutor(max_workers=max(1, MCESD_SEARCH_WORKERS)) as pool:
        for i in range(0, len(zips), batch):
            if _past_deadline():
                return None, False
            if limit and len(permits) >= limit:
                bounded = True
                break
            chunk = zips[i:i + batch]
            for rows in pool.map(lambda z: _mcesd_search_zip(session_for(), z, pacer, stats), chunk):
                scanned += 1
                rows_seen += len(rows)
                for row in rows:
                    row = row if isinstance(row, dict) else {}
                    p = _mcesd_permit(row)
                    if p is None:
                        ptype = ' '.join(str(row.get('permitType') or '').split())
                        if ptype in MCESD_FOOD_PERMIT_TYPES and not str(row.get('permitId') or '').upper().startswith('MF'):
                            junk += 1            # a food row without a usable name or street
                        else:
                            other_types[ptype or '(no type)'] += 1
                    elif p['permit'] not in permits:
                        permits[p['permit']] = p
    stats.bump('zips_scanned', scanned)
    stats.bump('permit_rows', rows_seen)
    stats.bump('junk_rows', junk)
    if junk:
        log.warning(f"  Maricopa: {junk} food permit rows skipped (no name or street in the search row)")
    if other_types:
        # Visible on purpose: a renamed permit type would otherwise vanish quietly.
        top = sorted(other_types.items(), key=lambda kv: -kv[1])[:8]
        log.info("  Maricopa: permit types not shipped: " + ', '.join(f"{t} ({n})" for t, n in top))
    return permits, bounded


def _mcesd_parse_permit_page(text):
    """The inspection rows of a permit page, or None when the page is not a
    complete permit page: no 'N Inspections Retrieved' marker, no table, a
    row without its seven cells and detail link, a date cell that is not
    'M/D/YYYY', or a row count that disagrees with the marker (a layout
    change must read as unread, never as a shorter history; the listing
    once skipped every row whose date it could not parse, so a new date
    format would have been a green empty run)."""
    m = MCESD_RETRIEVED_RE.search(text or '')
    if not m:
        return None
    expected = int(m.group(1))
    body = MCESD_TBODY_RE.search(text)
    if not body:
        return None
    rows = []
    for tr in MCESD_TR_RE.findall(body.group(1)):
        cells = MCESD_TD_RE.findall(tr)
        link = MCESD_LINK_RE.search(tr)
        if len(cells) < 6 or not link:
            return None
        try:
            datetime.strptime(_mcesd_text(cells[0]), '%m/%d/%Y')
        except ValueError:
            return None
        rows.append({'date': _mcesd_text(cells[0]), 'purpose': _mcesd_text(cells[1]),
                     'grade': _mcesd_text(cells[2]), 'status': _mcesd_text(cells[3]),
                     'priority': _mcesd_text(cells[4]),
                     'permit': html_lib.unescape(link.group(1)),
                     'inspection': html_lib.unescape(link.group(2))})
    if len(rows) != expected:
        return None
    return rows


def _mcesd_fetch_permit_page(session, permit, pacer, stats):
    """Rows of one permit page, or None when it could not be read completely
    (one re-fetch of a 200 without the markers, as of any other failure)."""
    url = MCESD_PERMIT_URL.format(permit=quote(permit, safe=''))
    rows = None
    for _ in range(2):
        r = _mcesd_request(session, 'GET', url, pacer=pacer, stats=stats)
        if r is not None and r.status_code == 200:
            rows = _mcesd_parse_permit_page(r.text)
            if rows is not None:
                return rows
        stats.bump('permit_refetches')
    return None


def _mcesd_citations(comments):
    """[(start, tier_match), ...] for every code citation in an entry's
    comments text, in order. One county item ('Violation #') can cite several
    Food Code sections, each with its own tier and 'Addl Notes' (12 of the 38
    entries on the 15 pages the first live run could not read): every tier
    marker (' - Priority:' / ' - Priority Foundation:' / ' - Core:', legacy
    ', P:' / ', Pf:' / ', C:') preceded within MCESD_CODE_REACH characters by
    a section citation such as '4-602.11 (E)' or 'PRIORITY VIOLATION-3-501.16
    (A)(2) and (B)' starts a citation there. None when a marker has no
    citation before it: a wording the parser does not know, never core."""
    out = []
    for m in MCESD_TIER_MARK_RE.finditer(comments):
        lo = max(0, m.start() - MCESD_CODE_REACH)
        codes = list(MCESD_CODE_RE.finditer(comments[lo:m.start()]))
        if not codes:
            return None
        out.append((lo + codes[-1].start(), m))
    return out


def _mcesd_violation(chunk):
    """[(number, title, severity, text), ...] for one 'Violation #:' entry,
    one tuple per code citation it carries; [] for an entry whose comments
    paragraph is PRESENT and empty (a re-inspection lists the re-checked
    item that way); None when the entry is not one the parser knows (then
    the whole page is unread): no number or description, text outside its
    comments and flag paragraphs beyond the labels, number and title, a
    flag paragraph carrying a citation, no comments paragraph at all (a
    renamed class must not read as an empty entry), or a citation's tier
    that cannot be read."""
    num_m = MCESD_VIOLATION_NUM_RE.search(chunk)
    desc_m = MCESD_DESC_RE.search(chunk)
    if not num_m or not desc_m:
        return None
    title = _mcesd_text(desc_m.group(1))
    number = _mcesd_text(num_m.group(1))
    if any(MCESD_CODE_RE.search(f) or MCESD_TIER_MARK_RE.search(f) for f in MCESD_FLAG_RE.findall(chunk)):
        return None                          # a citation printed where only a flag belongs
    rest = _mcesd_text(MCESD_FLAG_RE.sub(' ', MCESD_PARA_RE.sub(' ', chunk)))
    if rest != _mcesd_text(f'Violation #: {number} Description: {title} Comments:'):
        return None                          # text in the entry the parser does not know
    raw_paras = MCESD_PARA_RE.findall(chunk)
    if not raw_paras:
        return None                          # no comments paragraph: a layout change, not an empty entry
    comments = ' '.join(p for p in (_mcesd_text(p) for p in raw_paras) if p)
    if not comments:
        return []
    cites = _mcesd_citations(comments)
    if not cites:
        return None
    out = []
    for i, (start, tier_m) in enumerate(cites):
        end = cites[i + 1][0] if i + 1 < len(cites) else len(comments)
        severity = MCESD_TIER[(tier_m.group(1) or tier_m.group(2)).lower()]
        body = comments[tier_m.end():end]
        # The code phrase after the tier ('Cooling.', 'Sanitizers, Criteria-
        # Chemicals') up to the county's AMC-toolbox marketing sentence; the
        # observation follows 'Addl Notes:' (2024+) or '>>>' (legacy) and
        # ends at the predefined comment / corrective action / correct-by.
        head = re.split(r'Addl Notes:|>>>', body, maxsplit=1)[0]
        phrase = re.split(r'\s+For a permanent fix|https?://', head, maxsplit=1)[0]
        phrase = ' '.join(phrase.split()).strip(' .,;:-')
        obs_m = re.search(r'Addl Notes:\s*(.*)', body, re.S)
        if obs_m:
            observation = obs_m.group(1)
        else:
            j = body.find('>>>')
            observation = body[j + 3:] if j >= 0 else ''
        observation = re.split(r'Predefined Comment:|Corrective Action:|Correct By:',
                               observation, maxsplit=1)[0]
        observation = ' '.join(observation.split()).strip()
        detail = observation or phrase
        if detail.lower().strip('.') == title.lower().strip('.'):
            detail = ''                      # the code phrase merely repeats the item title
        text = f'{title}: {detail}' if title and detail else (title or detail)
        out.append((number, title, severity, text[:500]))
    return out


def _mcesd_violations_block(text):
    """The inner HTML of div#Violations up to its OWN closing tag (nested
    divs counted, so an inner div the county might add can never cut the
    block short and make a page read as clean), or None when the div is
    missing or never closed."""
    m = MCESD_VIOLATIONS_OPEN_RE.search(text or '')
    if not m:
        return None
    depth = 1
    for t in MCESD_DIV_TAG_RE.finditer(text, m.end()):
        depth += -1 if t.group(1) else 1
        if depth == 0:
            return text[m.end():t.start()]
    return None


def _mcesd_parse_detail(text):
    """{'header': {...}, 'violations': [(num, title, severity, text), ...],
    'empty_entries': n, 'priority_mismatch': bool} for an inspection detail
    page, or None when the page is not a complete one: missing header labels
    or div#Violations (the 'Inspection Not Available' page, an error page),
    text in the block that is not a violation entry, a block headed
    'Violations' with no entry the parser can see, or an entry it does not
    know (see _mcesd_violation). A whitespace-only div#Violations with the
    header present is a real clean inspection. The header's 'Priority
    Violation' tally ABOVE the Priority citations read is unread too (every
    such page of the first live run was a second citation the parser had
    missed); a tally below them is reported, not enforced: the county does
    not tally a suspension's or a re-inspection's citations, nor one it
    saw corrected on site."""
    header = {_mcesd_text(k): _mcesd_text(v) for k, v in MCESD_HEADER_RE.findall(text or '')}
    if 'Inspection Date' not in header or 'Inspection Status' not in header:
        return None
    block = _mcesd_violations_block(text)
    if block is None:
        return None
    chunks = re.split(r'(?=<p[^>]*>\s*<span class="fw-bold">Violation #:</span>)', block)
    violations, empty, entries = [], 0, 0
    for chunk in chunks:
        if 'Violation #:' not in chunk:
            if _mcesd_text(chunk) not in ('', 'Violations'):
                return None                 # text in the block that is not a violation
            continue
        entries += 1
        v = _mcesd_violation(chunk)
        if v is None:
            return None
        if not v:
            empty += 1
        violations.extend(v)
    if not entries and MCESD_VIOLATIONS_H2_RE.search(block):
        return None                         # headed 'Violations', yet no entry the parser can see
    stated = header.get('Priority Violation', '')
    parsed_p = sum(1 for v in violations if v[2] == 'priority')
    if stated.isdigit() and int(stated) > parsed_p:
        return None                         # a Priority citation the parser could not see
    return {'header': header, 'violations': violations, 'empty_entries': empty,
            'priority_mismatch': bool(stated.isdigit() and int(stated) != parsed_p)}


def _mcesd_points(permit_ids, stats=None):
    """permitId -> (lat, lng) from the ArcGIS facilities layer, one point per
    license (the layer repeats each ~4x). A failed query leaves its permits
    without a point; main() geocodes those and the merge coalesces."""
    points = {}
    ids = sorted(set(permit_ids))
    for i in range(0, len(ids), MCESD_ARCGIS_CHUNK):
        chunk = ids[i:i + MCESD_ARCGIS_CHUNK]
        where = 'LicenseNumber IN (' + ','.join(
            "'" + p.replace("'", "''") + "'" for p in chunk) + ')'
        if stats is not None:
            stats.bump('arcgis_queries')
        try:
            feats = _arcgis_query_all(MCESD_ARCGIS_LAYER, where=where, out_fields='LicenseNumber')
        except Exception as e:  # noqa: BLE001 - coordinates are supplementary; geocoded downstream
            log.warning(f"  Maricopa: ArcGIS points query failed for {len(chunk)} permits ({e})")
            continue
        for f in feats:
            lic = ' '.join(str((f.get('attributes') or {}).get('LicenseNumber') or '').split())
            g = f.get('geometry') or {}
            lat, lng = g.get('y'), g.get('x')
            if (lic and lic not in points and isinstance(lat, (int, float))
                    and isinstance(lng, (int, float)) and 31.0 < lat < 37.5 and -115.0 < lng < -108.5):
                points[lic] = (float(lat), float(lng))
    return points


def fetch_maricopa(since_date=None, limit=None):
    """
    Fetch Phoenix / Maricopa County, AZ inspections from the county's
    Restaurant Ratings Tool (see the section comment): zip inventory ->
    permit pages -> inspection detail pages, plus ArcGIS coordinates.
    original_score is None (no number is published); risk_score is
    100 - 5P - 2PF - C from the tiers the county states on every line.
    """
    log.info(f"Fetching Maricopa County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    floor = (datetime.now() - timedelta(days=MCESD_HISTORY_DAYS)).strftime('%Y-%m-%d')
    # requested_start is what the holds refer to (it repeats run to run while
    # held, so the give-up count works); window_start is what ships, reaching
    # back the posting-lag margin (see MCESD_POSTING_LAG_DAYS).
    requested_start = since or floor
    lag_start = (datetime.now() - timedelta(days=MCESD_POSTING_LAG_DAYS)).strftime('%Y-%m-%d')
    window_start = min(requested_start, lag_start) if since else floor
    if window_start != requested_start:
        log.info(f"  Maricopa: window widened from {requested_start} to {window_start} "
                 f"for the county's posting lag")
    stats = _McesdStats()
    started = time.time()
    local = threading.local()

    def session_for():
        if not hasattr(local, 's'):
            local.s = requests.Session()
        return local.s

    # 1. Inventory: every food permit, or raise.
    zips = _mcesd_zip_list()
    permits, bounded = _mcesd_inventory(session_for, zips, limit, stats)
    if permits is None:
        _set_group_status(MCESD_GROUP, 'skipped', 'fetch deadline reached during the permit inventory')
        log.warning("  Maricopa: fetch deadline reached during the permit inventory; nothing shipped")
        return []
    # A scan the limit did not stop saw every zip: too few permits means the
    # search broke (a renamed form field answers 200 with an empty list for
    # every zip), whatever the limit was. The weekly run passes 50,000.
    if not bounded and len(permits) < MCESD_MIN_FOOD_PERMITS:
        raise RuntimeError(f"Maricopa inventory implausibly small: {len(permits)} food permits "
                           f"from {stats.counts['zips_scanned']} zips (floor {MCESD_MIN_FOOD_PERMITS})")
    t_inventory = time.time() - started
    log.info(f"  Maricopa: {len(permits)} food permits ({stats.counts['permit_rows']} rows, "
             f"{stats.counts['zips_scanned']} zips) in {t_inventory:.0f}s")

    # 2. Listing: one permit page per food permit, oldest permit ids first
    # (deterministic); rows inside the window become detail candidates.
    pacer = _McesdPacer(MCESD_LISTING_RATE)
    ordered = sorted(permits.values(), key=lambda p: (p['zip'], p['permit']))
    candidates, seen = [], set()
    unread, newest, read_pages, cut = [], '', 0, None
    last_logged = 0
    skipped_known = implausible_dates = 0
    skipped_purpose = defaultdict(int)
    streak = 0
    t0 = time.time()
    batch = max(1, MCESD_LISTING_WORKERS) * 5
    with ThreadPoolExecutor(max_workers=max(1, MCESD_LISTING_WORKERS)) as pool:
        for i in range(0, len(ordered), batch):
            if _past_deadline():
                cut = 'deadline'
            elif streak >= MCESD_BREAKER_TRIP:
                cut = 'blocked'
            elif limit and len(candidates) >= limit:
                cut = 'limit'
            if cut:
                not_read = len(ordered) - i
                break
            chunk = ordered[i:i + batch]
            for permit, rows in zip(chunk, pool.map(
                    lambda p: _mcesd_fetch_permit_page(session_for(), p['permit'], pacer, stats), chunk)):
                if rows is None:
                    unread.append(permit['permit'])
                    streak += 1
                    continue
                streak = 0
                read_pages += 1
                for row in rows:
                    d = _mcesd_date(row['date'])
                    if not d:
                        implausible_dates += 1   # well-formed (the page parser checked), out of range: a typo
                        continue
                    if d > newest:
                        newest = d
                    if d < window_start:
                        continue
                    key = (row['permit'], row['inspection'])
                    if key in seen:
                        continue
                    seen.add(key)
                    if MCESD_SKIP_PURPOSE_RE.search(row['purpose'] or ''):
                        skipped_purpose[row['purpose']] += 1
                        continue
                    if SKIP_KNOWN_INSPECTIONS and (
                            make_restaurant_id(permit['name'], permit['address'], permit['city']),
                            d) in SKIP_KNOWN_INSPECTIONS:
                        skipped_known += 1
                        continue
                    candidates.append((d, permit, row))
            if read_pages - last_logged >= 2000:
                last_logged = read_pages
                log.info(f"  Maricopa: {read_pages} permit pages read, {len(candidates)} inspections "
                         f"in the window so far ({time.time() - t0:.0f}s)")
        else:
            not_read = 0
    t_listing = time.time() - t0
    log.info(f"  Maricopa: {read_pages} permit pages read ({len(unread)} unread) in {t_listing:.0f}s; "
             f"{len(candidates)} inspections since {window_start}"
             + (f", {skipped_known} already stored" if skipped_known else '')
             + (f"; newest row {newest}" if newest else ''))
    if skipped_purpose:
        log.info("  Maricopa: visits that are not inspections left out: " + ', '.join(
            f"{k} ({n})" for k, n in sorted(skipped_purpose.items(), key=lambda kv: -kv[1])))
    if implausible_dates:
        log.warning(f"  Maricopa: {implausible_dates} permit page rows dated before 2000 or in the "
                    f"future left out")
    if unread:
        # Tried twice and still not a complete permit page: that permit's
        # window is unknown. Hold the window on the permit so next run
        # re-reads it (given up after FRESHNESS_HOLD_GIVE_UP_RUNS runs) and
        # go red: a silently missing permit is how coverage gaps hide. The
        # pages a block ate were not that permit's failure: held, not counted.
        for pid in unread:
            _hold_window_start(MCESD_SOURCE, requested_start, counted=cut != 'blocked', key=pid)
        log.warning(f"  Maricopa: {len(unread)} permit pages could not be read: " + ', '.join(unread))
    if cut in ('deadline', 'blocked'):
        _hold_window_start(MCESD_SOURCE, requested_start, counted=False)
        reason = ('fetch deadline reached' if cut == 'deadline'
                  else f'portal blocked ({MCESD_BREAKER_TRIP} permit pages in a row unreadable)')
        _set_group_status(MCESD_GROUP, 'skipped' if cut == 'deadline' else 'blocked',
                          f"{reason}; {not_read} of {len(ordered)} permit pages not read")
        log.warning(f"  Maricopa: {reason}; nothing shipped this run")
        _MCESD_LAST_STATS.clear()
        _MCESD_LAST_STATS.update(stats.counts, permits=len(permits), listing_s=round(t_listing),
                                 inventory_s=round(t_inventory), cut=cut)
        return []
    problems = []
    if unread:
        problems.append(f"{len(unread)} of {len(ordered)} permit pages unreadable; "
                        f"their window is held and re-read next run")
    if cut is None and not bounded:
        _note_source_lag(MCESD_GROUP, newest)    # every permit page of every permit was read

    # 3. Details, oldest first so a deadline cut drops the newest tail.
    candidates.sort(key=lambda c: (c[0], c[1]['permit'], c[2]['inspection']), reverse=True)
    if limit:
        candidates = candidates[:limit]
    candidates.sort(key=lambda c: (c[0], c[1]['permit'], c[2]['inspection']))
    details = {}
    failures = defaultdict(int)
    blocked = threading.Event()
    streak_lock = threading.Lock()
    streak_box = [0]
    t0 = time.time()

    def work(cand):
        d, permit, row = cand
        if _past_deadline():
            return cand, 'deadline'
        if blocked.is_set():
            return cand, 'blocked'
        url = MCESD_INSPECTION_URL.format(permit=quote(row['permit'], safe=''),
                                          inspection=quote(row['inspection'], safe=''))
        r = _mcesd_request(session_for(), 'GET', url, stats=stats)
        time.sleep(MCESD_DETAIL_DELAY)
        if r is None or r.status_code != 200:
            outcome = 'download'
        elif MCESD_UNAVAILABLE in r.text:
            outcome = 'unavailable'     # the app no longer serves this (old) inspection
        else:
            parsed = _mcesd_parse_detail(r.text)
            outcome = parsed if parsed is not None else 'unread'
        with streak_lock:
            if isinstance(outcome, dict):
                streak_box[0] = 0
            elif outcome in ('download', 'unread', 'unavailable'):
                # any page that gave no inspection, a 'Not Available' one too:
                # MCESD_BREAKER_TRIP of them in a row is the portal, not the page
                streak_box[0] += 1
                if streak_box[0] >= MCESD_BREAKER_TRIP:
                    blocked.set()
        return cand, outcome

    dropped_ids = defaultdict(list)
    with ThreadPoolExecutor(max_workers=max(1, MCESD_DETAIL_WORKERS)) as pool:
        for cand, outcome in pool.map(work, candidates):
            d, permit, row = cand
            if isinstance(outcome, dict):
                details[(row['permit'], row['inspection'])] = outcome
                continue
            failures[outcome] += 1
            tried = outcome in ('download', 'unread', 'unavailable')
            if tried:
                dropped_ids[outcome].append(f"{row['permit']}/{row['inspection']}")   # every id: a blind spot is diagnosed from the log
            # Dropped, never shipped clean. Tried-and-failed drops count
            # toward the per-record give-up; a deadline or block cut does not.
            _hold_freshness(MCESD_SOURCE, d, counted=tried, key=row['inspection'])
    t_details = time.time() - t0
    log.info(f"  Maricopa: {len(details)} detail pages read in {t_details:.0f}s "
             f"({MCESD_DETAIL_WORKERS} workers)")
    not_read = failures.get('unread', 0) + failures.get('unavailable', 0)
    status = None
    if failures.get('deadline'):
        status = 'skipped'
        problems.append(f"fetch deadline reached; {failures['deadline']} detail pages not read")
    elif failures.get('blocked'):
        status = 'blocked'
        problems.append(f"portal blocked ({MCESD_BREAKER_TRIP} detail pages in a row unreadable); "
                        f"{failures['blocked']} detail pages not read")
    if not_read >= MCESD_UNREAD_ERROR_MIN:
        # Pages the portal answered that gave no inspection are a parser
        # blind spot or a portal change, never a transient: red, with every
        # id in the log, before the holds give them up.
        status = status or 'error'
        problems.append(f"{not_read} detail pages answered but gave no inspection "
                        f"({failures.get('unread', 0)} not readable, {failures.get('unavailable', 0)} "
                        f"'Inspection Not Available'); held, ids in the log")
    if problems:
        _set_group_status(MCESD_GROUP, status or 'error', '; '.join(problems))
    if failures:
        log.warning("  Maricopa: dropped " + ', '.join(
            f"{n} ({why})" for why, n in sorted(failures.items())) +
            " inspections whose detail page could not be read (held for the next run)" +
            ('; ' + '; '.join(f"{why}: {', '.join(ids)}" for why, ids in sorted(dropped_ids.items()))
             if dropped_ids else ''))

    # Coordinates for the permits that ship.
    shipped = [(permit, row) for (_, permit, row) in candidates
               if (row['permit'], row['inspection']) in details]
    points = _mcesd_points([permit['permit'] for permit, _ in shipped] +
                           [row['permit'] for _, row in shipped], stats)

    results = []
    date_mismatch = priority_mismatch = empty_entries = 0
    for d, permit, row in candidates:
        parsed = details.get((row['permit'], row['inspection']))
        if parsed is None:
            continue
        header = parsed['header']
        # The permit page's row is the index this run filtered and held on,
        # so its date is the one that ships; the detail page is checked.
        page_date = _mcesd_date(header.get('Inspection Date'))
        if page_date and page_date != d:
            date_mismatch += 1
        priority_mismatch += 1 if parsed.get('priority_mismatch') else 0
        empty_entries += parsed.get('empty_entries', 0)
        violations = []
        for _num, _title, severity, text in parsed['violations']:
            cat, _, _ = classify_violation(text)
            violations.append([cat, severity, text])
        risk, pv, pfv, cv = calc_risk_score(violations)
        point = points.get(permit['permit']) or points.get(row['permit'])
        results.append({
            'name': permit['name'],
            'address': permit['address'],
            'city': permit['city'] or 'Maricopa County',
            'state': 'AZ',
            'zip': permit['zip'],
            'latitude': point[0] if point else None,
            'longitude': point[1] if point else None,
            'inspection_date': d,
            'original_score': None,         # the county publishes a grade card, not a number
            'risk_score': risk,
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': pv + pfv + cv,
            'violations': violations,
            'inspection_type': header.get('Inspection Purpose') or row['purpose'],
            'results': _mcesd_results(header.get('Grade') or row['grade'],
                                      header.get('Inspection Status') or row['status']),
            'source': MCESD_SOURCE,
            'source_url': MCESD_INSPECTION_URL.format(permit=quote(row['permit'], safe=''),
                                                      inspection=quote(row['inspection'], safe='')),
            'source_id': f"mcesd_{row['permit']}_{row['inspection']}",
            'metro': MCESD_METRO,
        })
    if date_mismatch:
        log.warning(f"  Maricopa: {date_mismatch} detail pages print a different inspection "
                    f"date than the permit page row (the row's date shipped)")
    if priority_mismatch or empty_entries:
        log.info(f"  Maricopa: {priority_mismatch} detail pages whose Priority tally is below their "
                 f"citations (the citations shipped); {empty_entries} violation entries printed "
                 f"with no comments (nothing to read, left out)")
    # Newest day first; within a day the worst visit first (an F, then the
    # lowest score), so same-day picks are deterministic.
    results.sort(key=lambda r: (r['inspection_date'], -r['risk_score'], r['priority_violations'],
                                r['total_violations'], r['source_id']), reverse=True)
    with_point = sum(1 for r in results if r['latitude'] is not None)
    _MCESD_LAST_STATS.clear()
    _MCESD_LAST_STATS.update(stats.counts, permits=len(permits), inventory_bounded=bounded,
                             permit_pages_read=read_pages,
                             permit_pages_unread=len(unread), candidates=len(candidates),
                             skipped_known=skipped_known, skipped_purpose=dict(skipped_purpose),
                             implausible_dates=implausible_dates,
                             details_read=len(details),
                             dropped=dict(failures), records=len(results), with_point=with_point,
                             newest=newest, inventory_s=round(t_inventory),
                             listing_s=round(t_listing), details_s=round(t_details),
                             total_s=round(time.time() - started), cut=cut)
    log.info(f"Maricopa County: {len(results)} inspections ({with_point} with ArcGIS coordinates) "
             f"in {time.time() - started:.0f}s, {stats.counts['requests']} portal requests "
             f"({stats.counts['retries']} retried after a 429/5xx/transport error)")
    return results


# ─── HARRIS COUNTY, TX (Harris County Public Health food safety API) ────────
# Harris County Public Health's Food Safety Program inspects the ~9,100
# permitted food establishments of unincorporated Harris County and its 23
# contract cities (Katy, Humble, Tomball, La Porte, Deer Park, Bellaire,
# Seabrook, Jersey Village, ...). The City of Houston runs its own program
# (fetch_houston) and is outside this data by construction; the 'Houston'
# mailing city on ~45% of the rows is an unincorporated address.
# The public "FSP portal" (secureweb.hcphtx.org/fspportal) is an Angular app
# over a JSON API with no auth, cookie or token: a DevExtreme establishment
# search (server-side filter, sort and skip/take, totalCount in every
# answer), a per-establishment inspection list (the permit's whole published
# history: the lists read in the 2026-11 probes start in 2024-11, and the
# oldest lastCompletedInspectionDate on file is 2022-02-01; only ACTIVE
# permits are searchable, so a closed place's history is out of reach) and
# a per-inspection list of the OUT-of-compliance items.
# There is no cross-establishment inspection list by date (probed
# 2026-11-02: every guessed route answers 405 or the SPA shell), so a window
# is read in three phases:
#   1. the establishments whose lastCompletedInspectionDate >= the window
#      start less HARRIS_LC_LAG_DAYS (that field trails the inspection
#      list, see the constant);
#   2. each one's inspection list, kept to the widened window and the visit
#      types below;
#   3. each kept inspection's items, oldest first.
# A search page that cannot be read raises (never a partial window); an
# unreadable inspection list or item list drops that establishment or
# inspection and holds the window on it, and more than
# HARRIS_LIST_FAILURES_MAX unreadable lists in one run raise; two
# contradiction checks (HARRIS_LIST_AGREE_DAYS, _harris_items_unread) keep
# an endpoint that answers [] from reading as a clean week;
# HARRIS_BREAKER_TRIP exhausted requests in a row trip a
# breaker so a blocked host cannot eat the fetch deadline one timeout at a
# time (the group is then reported 'blocked', with holds).
# Score: the county's 'score' is 100 minus the sum of the items' demerits
# (an OUT item costs 1-3), copied to original_score and risk_score. Severity
# is the county's violationGroup — P (priority), F (priority foundation), C
# (core) — registered in jev_reader SOURCE_TIER_SOURCES so Jev never
# re-tiers it. The items carry only their form title (no observation text),
# emitted bare as "<item no>. <title>" so jev_reader's exact-title tables
# (FORM_TITLE_PHRASES, CITED_PEST_ITEM_TITLES) read them.
# Coordinates exist on 39 of 9,138 establishments: latitude/longitude are
# None where absent and main() geocodes the address.
# Establishment rows carry the owner's name, phone, e-mail, id number and
# sales-tax id, inspection rows the inspector's name: only the fields read
# below leave this section, raw rows are never logged, and the test fixtures
# are scrubbed of those fields.

HARRIS_BASE = 'https://secureweb.hcphtx.org/fspportal/'
HARRIS_SEARCH_URL = HARRIS_BASE + 'api/establishment/search'
HARRIS_INSPECTIONS_URL = HARRIS_BASE + 'api/inspection/get-inspections-for-establishment'
HARRIS_VIOLATIONS_URL = HARRIS_BASE + 'api/inspection/get-violations-for-inspection'
HARRIS_ESTABLISHMENT_URL = HARRIS_BASE + 'establishment/{}'   # the app's own route
HARRIS_SOURCE = 'Harris County Public Health'
HARRIS_GROUP = 'Harris County'
# No metro chip: the City of Houston's own records (fetch_houston) carry '',
# and a 'Houston' chip listing only the suburbs would mislead. Set both to
# 'Houston' together if a Houston-area chip is wanted.
HARRIS_METRO = ''
HARRIS_DEFAULT_CITY = 'Houston'
HARRIS_PAGE = 500            # take= per search page; a week is 300-550 rows
HARRIS_TIMEOUT = 60
HARRIS_RETRIES = 4           # attempts per request
HARRIS_BACKOFF = 5           # seconds between attempts: 5, 15, 45
HARRIS_WORKERS = 3
HARRIS_DELAY = 0.4           # seconds after every request, per worker
HARRIS_BREAKER_TRIP = 3      # exhausted requests in a row -> _HARRIS_BLOCKED
HARRIS_CLOSURE_CAP = 55      # risk_score ceiling when the county ordered a closure
HARRIS_LATE_ENTRY_DAYS = 2   # an inspection entered this long after its date is counted as late
# A per-record failure (an inspection list or item list that cannot be read)
# is skipped and held on its own (key = documentId / inspection number) so
# FRESHNESS_HOLD_GIVE_UP_RUNS tried-and-failed runs give a permanently bad
# record up (Omaha has two such records) instead of failing the county every
# week; more than this many inspection lists failing in one run is an outage
# and the window is not shipped.
HARRIS_LIST_FAILURES_MAX = 5
# Two contradiction checks guard the two JSON answers that would read as
# "nothing here" if the endpoint stopped answering anonymous callers or
# renamed a field. An establishment's inspection list must hold a row dated
# within HARRIS_LIST_AGREE_DAYS of the lastCompletedInspectionDate the search
# listed it for: of 1,540 live establishments on 2026-11-03, 1,501 agreed to
# the day, 36 within 3 days, none had an empty list, and 3 had a field 48-162
# days past the list's newest row with the permit-approval or next-inspection
# date moved on the field's day — an inspection recorded but not yet
# published. Such an establishment is held on its own (key = documentId) so
# the next runs re-read it until it publishes or FRESHNESS_HOLD_GIVE_UP_RUNS
# give it up; a gap shorter than the tolerance is re-read by the
# HARRIS_LC_LAG_DAYS margin anyway, which is why the two are about equal. An
# inspection scored below 100 must come with an OUT item
# (_harris_items_unread). Once HARRIS_MASS_FAILURE_MIN were checked and more
# than half contradict, nothing ships and the group is an error.
HARRIS_LIST_AGREE_DAYS = 30
HARRIS_MASS_FAILURE_MIN = 20
# The search filter field trails the inspection list. lastCompletedInspectionDate
# equalled the newest listed inspection on 97-98% of the establishments probed
# on 2026-11-03 (454 of 467 inspected in the previous 14 days; 231 of 240
# whose field was 1.5-5.5 months old); on the rest an inspection 5-11 days
# NEWER than the field — a Follow-up, Consultation, Permitting or Food Borne
# Illness visit shortly after a reflected one, Approved and Complete — had
# still not moved it weeks or months later. So a window is read from this
# many days before its start: the search still finds an establishment whose
# field is stuck on the earlier visit, and the later visit, dated inside the
# widened window, ships; inspections already shipped from the margin are
# upserted again (idempotent). Every observed gap was 5-11 days; an
# unreflected visit more than this margin after the previous reflected one
# is missed until the establishment's next reflected inspection.
HARRIS_LC_LAG_DAYS = 28
# A score of 100 means no OUT item (every OUT item costs at least one
# demerit); the live checks of 2026-11-03 read the items of 105 score-100
# inspections and every list was empty, so their item call is skipped. (On
# ~1% of inspections the county's score sits exactly 2 above 100 minus the
# demerits, so a 100 with one 2-demerit item is conceivable; it would ship
# as the clean A it grades as either way, missing only that item's chip.)
HARRIS_READ_CLEAN_ITEMS = False
HARRIS_HEADERS = {
    'User-Agent': CHROME_UA,
    'Accept': 'application/json, text/plain, */*',
    'App-Version': '1.0',
    'Origin': 'https://secureweb.hcphtx.org',
    'Referer': HARRIS_BASE + 'search',
}
HARRIS_GROUP_SEVERITY = {'P': 'priority', 'F': 'priority_foundation', 'C': 'core'}
# Visit types (the 14-day window of 2026-10-20..11-02 held 587 inspections:
# Permitting 225, Routine Inspection 155, School Inspection 64, Pre-opening
# 40, Complaint 31, Consultation 21, Follow-up 17, Food Borne Illness 15,
# Daycare Kitchen Inspection 14, Follow-Up Inspection 3, SV Follow-up 1,
# Zero/Zero 1). 'Permitting' is the county's annual permit inspection — a
# full scored inspection of an operating establishment, done 0-3 months
# before the permit expires, and for many low-risk permits (convenience
# stores, retail) the only inspection they ever get — so it ships like a
# routine. 'Pre-opening' is the approval visit before a new establishment
# opens and never ships; nor does 'Non-permitted' (the visit to a place
# operating before its permit exists), 'Consultation' (advisory, scored 100
# in nearly every sample) or the institutional school/day-care kitchen
# visits (their establishments are out of scope anyway). A follow-up ships
# only once the permit has an operating-type inspection on record: a
# 'Follow-up' right after a 'Pre-opening' is the pre-opening re-check. Any
# other type is dropped with a warning so a new one gets a decision.
# A Complete inspection ships whatever its approvalStatus (the supervisor's
# review of the report: 'Approved', 'Pending' on about half of the newest
# reports, 'Rejected' on ~7%; a rejected report keeps its items and its
# follow-up). A report revised after review keeps its number and date, and
# the HARRIS_LC_LAG_DAYS re-read upserts the revision over the first
# version; a withdrawn one stays, as for every source (no delete path).
HARRIS_OPERATING_TYPES = frozenset({'Routine Inspection', 'Complaint', 'Food Borne Illness', 'Permitting'})
HARRIS_FOLLOW_UP_TYPES = frozenset({'Follow-up', 'Follow-Up Inspection', 'SV Follow-up'})
HARRIS_DROP_TYPES = frozenset({'Pre-opening', 'Non-permitted', 'Consultation',
                               'School Inspection', 'Daycare Kitchen Inspection'})
HARRIS_PERMIT_TYPES = frozenset({'Permitting'})      # a 'No' approval is worth a results note
HARRIS_TYPE_DISPLAY = {'Routine Inspection': 'Routine', 'Follow-Up Inspection': 'Follow-up'}
# Scope: food businesses the public walks into. Institutional kitchens,
# child care, schools, pantries/warehouses, the mobile units' commissaries
# (MFU Central Prep Facility) and the mobile-unit entries (whose address is
# the commissary or the owner) are skipped, as is any establishment whose
# type is not 'Fixed' (Mobile, FarmersMarket). A caterer with a fixed
# kitchen is a food business and ships.
HARRIS_SKIP_SERVICE_TYPES = frozenset({
    'Public School', 'School', 'Day Care Facility', 'Long Term Care', 'Hospital',
    'Detention Facility', 'Residential Shelter', 'Food Pantry/Food Storage Warehouse',
    'MFU Service Area', 'MFU Central Prep Facility', 'Mobile Food Unit',
    "Farmer's Market Vendor", 'Farmer’s Market Vendor',
})

_HARRIS_LOCK = threading.Lock()
_HARRIS_BLOCKED = False          # breaker: set once HARRIS_BREAKER_TRIP requests in a row exhausted
_HARRIS_EXHAUSTED_STREAK = 0
_HARRIS_FAILURES_LOGGED = 0
_HARRIS_LAST_ERROR = ''
_HARRIS_STATS = {}               # this fetch's request count and outcomes (logs and tests)


class _HarrisCut(Exception):
    """A phase stopped early: the breaker tripped ('blocked') or the fetch
    deadline passed ('skipped'). fetch_harris turns it into the group
    status plus a window hold."""

    def __init__(self, status, detail):
        super().__init__(detail)
        self.status, self.detail = status, detail


def _harris_stat(key, n=1):
    with _HARRIS_LOCK:
        _HARRIS_STATS[key] = _HARRIS_STATS.get(key, 0) + n


def _harris_request(session, url, body=None, params=None, info=None):
    """JSON from one HCPH API call (a POST of the JSON `body`, else a GET
    with `params`), or None once HARRIS_RETRIES attempts failed. Retries wait
    HARRIS_BACKOFF * 3**attempt seconds (a 429/503 Retry-After when longer),
    never past the fetch deadline. `info` (a dict) says why None came back:
    'permanent' — a 400/404/410, that request's own problem, not retried and
    not counted; 'exhausted' — every attempt failed (a 5xx, a timeout, a
    403, the SPA shell in place of JSON), which counts toward the breaker:
    HARRIS_BREAKER_TRIP in a row set _HARRIS_BLOCKED and every further
    request returns None at once with no flag; 'deadline' — the retry ladder
    was cut by the fetch deadline."""
    global _HARRIS_BLOCKED, _HARRIS_EXHAUSTED_STREAK, _HARRIS_FAILURES_LOGGED, _HARRIS_LAST_ERROR
    last_err, permanent, deadline = None, False, False
    for attempt in range(HARRIS_RETRIES):
        if _HARRIS_BLOCKED:
            return None
        _harris_stat('requests')
        try:
            if body is not None:
                r = session.post(url, data=json.dumps(body), timeout=HARRIS_TIMEOUT,
                                 headers={**HARRIS_HEADERS, 'Content-Type': 'application/json'})
            else:
                r = session.get(url, params=params, timeout=HARRIS_TIMEOUT, headers=HARRIS_HEADERS)
            r.raise_for_status()
            data = r.json()
            with _HARRIS_LOCK:
                _HARRIS_EXHAUSTED_STREAK = 0
            return data
        except (requests.RequestException, ValueError) as e:
            last_err = e
            resp = getattr(e, 'response', None)
            status = resp.status_code if resp is not None else None
            if status in (400, 404, 410):
                permanent = True
                break
            if attempt == HARRIS_RETRIES - 1:
                break
            if _past_deadline():
                deadline = True
                break
            wait = HARRIS_BACKOFF * (3 ** attempt)
            if status in (429, 503):
                try:
                    wait = max(wait, min(int((resp.headers or {}).get('Retry-After', 0)), 300))
                except (TypeError, ValueError):
                    pass
            time.sleep(wait)
    _HARRIS_LAST_ERROR = f'{type(last_err).__name__}: {str(last_err)[:120]}' if last_err else 'breaker tripped'
    if info is not None:
        info['permanent' if permanent else 'deadline' if deadline else 'exhausted'] = True
    if permanent:
        _harris_stat('permanent failures')
    elif deadline:
        _harris_stat('deadline cuts')
    elif last_err is not None:
        _harris_stat('exhausted failures')
        with _HARRIS_LOCK:
            _HARRIS_EXHAUSTED_STREAK += 1
            if _HARRIS_EXHAUSTED_STREAK >= HARRIS_BREAKER_TRIP and not _HARRIS_BLOCKED:
                _HARRIS_BLOCKED = True
                log.error(f"  Harris County: {_HARRIS_EXHAUSTED_STREAK} requests in a row failed "
                          f"through every retry ({_HARRIS_LAST_ERROR}) — treating it as a block")
    if _HARRIS_FAILURES_LOGGED < 5:
        _HARRIS_FAILURES_LOGGED += 1
        log.warning(f"  Harris County request failed ({url.replace(HARRIS_BASE, '<portal>/')}"
                    f"{'' if params is None else ' ' + str(params)}): {_HARRIS_LAST_ERROR}")
    return None


def _harris_clean(text):
    """Collapse whitespace; title-case an ALL CAPS value (keeping "Joe's")."""
    s = ' '.join(str(text or '').split())
    if s.isupper():
        s = re.sub(r"'S\b", "'s", s.title())
    return s


def _harris_coord(v):
    """A float coordinate, or None for a missing or zero value."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f and f == f else None


def _harris_flag(v):
    """The API prints its booleans as 'true'/'false' strings, 0/1 or 'Yes'/'No'."""
    return str(v or '').strip().lower() in ('true', '1', 'yes')


def _harris_search_body(since, skip, take):
    body = {'skip': skip, 'take': take, 'requireTotalCount': True,
            # newest first so a bounded (test-mode) read sees the newest
            # establishments; documentId keeps page boundaries stable
            'sort': [{'selector': 'lastCompletedInspectionDate', 'desc': True},
                     {'selector': 'documentId', 'desc': False}]}
    if since:
        body['filter'] = [['lastCompletedInspectionDate', '>=', f'{since}T00:00:00']]
    return body


def _harris_list_establishments(session, since, limit):
    """Phase 1: every active establishment whose newest inspection is on or
    after `since` (all of them when None), newest first, deduped on
    documentId. Newest first so a bounded (test-mode) read sees the newest
    establishments. There is no snapshot: an establishment re-inspected
    between two pages moves to the top of the listing — a row already read
    comes back once more (deduped), a row not yet read jumps past the cursor
    and is missed — so pages are followed until a short page reaches the
    server's totalCount or an empty page comes back (a server that clamped
    `take` must not end a window early) and the UNIQUE count is checked
    against totalCount unless the read stopped at `limit`: such a run fails
    closed and the self-healing lookback re-reads the window next week. (A
    weekly window fits one page, so the exposure is the seconds between two
    pages.) A server that stops paging — the same rows again, or rows still
    coming a whole page past its own totalCount — fails closed too instead
    of spinning until the fetch deadline. Raises RuntimeError when a page
    cannot be read or the listing came up short; _HarrisCut on the deadline
    or the breaker."""
    take = min(HARRIS_PAGE, max(int(limit), 1)) if limit else HARRIS_PAGE
    ests, skip, total, pages = {}, 0, 0, 0
    while True:
        if _past_deadline():
            raise _HarrisCut('skipped', f'fetch deadline reached while listing establishments '
                                        f'({len(ests)} read)')
        info = {}
        page = _harris_request(session, HARRIS_SEARCH_URL, body=_harris_search_body(since, skip, take),
                               info=info)
        rows = page.get('data') if isinstance(page, dict) else None
        if not isinstance(rows, list):
            if _HARRIS_BLOCKED:
                raise _HarrisCut('blocked', f'HCPH stopped answering while listing establishments '
                                            f'({_HARRIS_LAST_ERROR})')
            if info.get('deadline') or _past_deadline():
                raise _HarrisCut('skipped', f'fetch deadline reached while listing establishments '
                                            f'({len(ests)} read, the page at skip={skip} not read)')
            raise RuntimeError(f'Harris County: establishment search page at skip={skip} could not '
                               f'be read ({_HARRIS_LAST_ERROR}); not shipping a partial window')
        try:
            total = int(page.get('totalCount'))
        except (TypeError, ValueError):
            raise RuntimeError(f'Harris County: establishment search answered without a totalCount '
                               f'(skip={skip}); not shipping a partial window')
        pages += 1
        new = 0
        for row in rows:
            did = row.get('documentId') if isinstance(row, dict) else None
            if did and did not in ests:
                ests[did] = row
                new += 1
        time.sleep(HARRIS_DELAY)
        skip += len(rows)
        if not rows or (limit and len(ests) >= limit) or (len(rows) < take and skip >= total):
            break
        if not new:
            raise RuntimeError(f'Harris County: establishment search is not paging (the page at '
                               f'skip={skip - len(rows)} added none of its {len(rows)} rows); not shipping '
                               f'a partial window')
        if skip >= total + take:
            raise RuntimeError(f'Harris County: establishment search paged {skip} rows past a totalCount '
                               f'of {total}; not shipping a partial window')
    # Stopping at `limit` is the only short listing that is intended; any
    # other count below totalCount is a page boundary that moved under the
    # read. (main() passes limit=50000 in weekly mode, so the check keys on
    # the limit having been reached, not on one being set.)
    bounded = bool(limit) and len(ests) >= limit
    if not bounded and len(ests) < total:
        raise RuntimeError(f'Harris County: establishment listing incomplete ({len(ests)} of {total} '
                           f'establishments in {pages} pages); not shipping a partial window')
    log.info(f"  Harris County: {len(ests)} establishments with an inspection since "
             f"{since or 'the start of the record'} ({pages} search pages, totalCount {total})")
    return list(ests.values())


def _harris_in_scope(est, skipped):
    """Food businesses only (see HARRIS_SKIP_SERVICE_TYPES); counts the rest."""
    kind = (est.get('type') or 'Fixed').strip()
    if kind != 'Fixed':
        skipped[f'type {kind}'] += 1
        return False
    service = ' '.join(str(est.get('serviceType') or '').split())
    if service in HARRIS_SKIP_SERVICE_TYPES:
        skipped[service] += 1
        return False
    if not _harris_clean(est.get('name')):
        skipped['no name'] += 1
        return False
    return True


def _harris_select_inspections(inspections, since, dropped):
    """From one establishment's whole inspection history: the Complete
    inspections dated on or after `since` whose visit type ships (see the
    type policy above), each with its 'date' normalised. Oldest first."""
    rows = []
    for insp in inspections:
        if not isinstance(insp, dict):
            dropped['malformed row'] += 1
            continue
        date = sane_inspection_date(insp.get('date'))
        number = ' '.join(str(insp.get('number') or '').split())
        if not date or not number:
            dropped['no date or number'] += 1
            continue
        rows.append((date, number, insp))
    rows.sort(key=lambda t: (t[0], t[1]))
    out, operating = [], False
    for date, number, insp in rows:
        kind = ' '.join(str(insp.get('type') or '').split())
        if kind in HARRIS_OPERATING_TYPES:
            ship, why = True, ''
        elif kind in HARRIS_FOLLOW_UP_TYPES:
            ship, why = operating, f'pre-opening {kind}'
        elif kind in HARRIS_DROP_TYPES:
            ship, why = False, kind
        else:
            ship, why = False, f'unknown type {kind or "(blank)"}'
        if kind in HARRIS_OPERATING_TYPES:
            operating = True
        if since and date < since:
            continue
        if not ship:
            dropped[why] += 1
            continue
        status = ' '.join(str(insp.get('status') or '').split())
        if status != 'Complete':
            dropped[f'status {status or "(blank)"}'] += 1
            continue
        out.append(dict(insp, date=date, number=number, type=kind))
    return out


def _harris_violation(item):
    """[category, severity, text] for one OUT item, or None for a row with no
    title. The county states the tier (violationGroup P/F/C); the classifier
    gives the category, and its severity only when the group is missing."""
    if not isinstance(item, dict):
        return None
    title = ' '.join(str(item.get('violationDescription') or '').split())
    if not title:
        return None
    result = str(item.get('complianceResult') or '').strip().lower()
    if result and result != 'out':
        return None
    num = item.get('violationNumber')
    text = f'{num}. {title}' if num not in (None, '') else title
    severity = HARRIS_GROUP_SEVERITY.get(str(item.get('violationGroup') or '').strip().upper())
    cat, classifier_severity, _ = classify_violation(text)
    return [cat, severity or classifier_severity, text[:500]]


def _harris_demerits(items):
    total = 0
    for item in items:
        if isinstance(item, dict):
            try:
                total += int(item.get('numberOfDemerits') or 0)
            except (TypeError, ValueError):
                pass
    return total


def _harris_score(insp):
    """The county's score as an int, or None when absent or not a number."""
    try:
        return int(insp.get('score'))
    except (TypeError, ValueError, AttributeError):
        return None


def _harris_items_unread(insp, items):
    """True when an inspection's item list was not really read: the county
    scored it below 100 — so at least one OUT item cost a demerit — yet no
    row of the answer is an OUT item with a title (an empty answer, a
    renamed field, a reworded compliance result). None of the 1,933 scored
    inspections read live on 2026-11-03 looked like this, so it is a layout
    change or a dead endpoint and never a clean inspection: the caller drops
    and holds it rather than ship the county's score with no violations."""
    score = _harris_score(insp)
    return score is not None and 0 < score < 100 and not any(_harris_violation(i) for i in items)


def _harris_record(est, insp, items, counts):
    """One record dict from an establishment row, one of its inspections and
    the inspection's OUT items; None for an unscored visit with no items."""
    violations = [v for v in (_harris_violation(i) for i in items) if v]
    demerits = _harris_demerits(items)
    score = _harris_score(insp)
    if score is None or score <= 0:
        if not violations:
            counts['unscored'] += 1
            return None
        original, risk = None, max(0, 100 - demerits)
    else:
        original, risk = score, max(0, min(100, score))
        if items and score != 100 - demerits:
            counts['score != 100 - demerits'] += 1
    closure = _harris_flag(insp.get('isClosureRequired'))
    if closure:
        risk = min(risk, HARRIS_CLOSURE_CAP)
    if closure:
        results = 'Closure required'
    elif insp['type'] in HARRIS_PERMIT_TYPES and str(insp.get('isPermitApproval') or '').strip().lower() == 'no':
        results = 'Permit not approved'
    elif _harris_flag(insp.get('isFollowUpRequired')):
        results = 'Follow-up required'
    else:
        results = ''
    try:
        entered = datetime.strptime(str(insp.get('dateUtc') or '')[:10], '%Y-%m-%d')
        if (entered - datetime.strptime(insp['date'], '%Y-%m-%d')).days > HARRIS_LATE_ENTRY_DAYS:
            counts['entered late'] += 1
    except ValueError:
        pass
    _, pv, pfv, cv = calc_risk_score(violations)
    # Only an exact two-letter code is kept: one live row spells out 'Texas'
    # (which a [:2] slice turned into 'TE'), and every permit is in Texas.
    state = ' '.join(str(est.get('state') or '').split()).upper()
    if not re.fullmatch(r'[A-Z]{2}', state):
        state = 'TX'
    return {
        'name': _harris_clean(est.get('name')),
        'address': _harris_clean(est.get('address')),
        'city': _harris_clean(est.get('city')) or HARRIS_DEFAULT_CITY,
        'state': state,
        'zip': str(est.get('zip') or '').strip()[:5],
        'latitude': _harris_coord(est.get('latitude')),
        'longitude': _harris_coord(est.get('longitude')),
        'inspection_date': insp['date'],
        'original_score': original,         # the county's 100 - demerits
        'risk_score': risk,
        'priority_violations': pv,
        'priority_foundation_violations': pfv,
        'core_violations': cv,
        'total_violations': len(violations),
        'violations': violations,
        'inspection_type': HARRIS_TYPE_DISPLAY.get(insp['type'], insp['type']),
        'results': results,
        'source': HARRIS_SOURCE,
        'source_url': HARRIS_ESTABLISHMENT_URL.format(est.get('documentId')),
        'source_id': f"hcph_{insp['number']}",
        'metro': HARRIS_METRO,
    }


def _harris_outcome(info):
    """Why a per-record request came back without data: 'gone' (the record
    no longer exists: 400/404/410), 'failed' (tried through every retry, or
    answered something that is not a list), 'deadline' (the retry ladder or
    the phase was cut by the fetch deadline) or 'blocked' (the breaker had
    already tripped, nothing was tried)."""
    if info.get('permanent'):
        return 'gone'
    if info.get('exhausted'):
        return 'failed'
    if info.get('deadline'):
        return 'deadline'
    if _HARRIS_BLOCKED:
        return 'blocked'
    return 'deadline' if _past_deadline() else 'failed'


def _harris_newest(session):
    """The source's own newest lastCompletedInspectionDate (for the lag
    note), '' when the probe fails."""
    try:
        page = _harris_request(session, HARRIS_SEARCH_URL, body={
            'skip': 0, 'take': 1, 'requireTotalCount': False,
            'sort': [{'selector': 'lastCompletedInspectionDate', 'desc': True}]})
        rows = page.get('data') if isinstance(page, dict) else None
        return str((rows or [{}])[0].get('lastCompletedInspectionDate') or '')[:10]
    except Exception as e:  # noqa: BLE001 - the probe is informational
        log.debug(f"  Harris County: newest-date probe failed: {e}")
        return ''


def fetch_harris(since_date=None, limit=None):
    """
    Fetch Harris County, TX inspections from Harris County Public Health's
    FSP portal API (see the section comment above): unincorporated Harris
    County and its 23 contract cities, one record per inspection, official
    score, county-stated P/F/C severity, item titles as the violation text.

    since_date is 'YYYY-MM-DD[T00:00:00]' or None (the whole published
    record: the per-establishment lists go back to 2024). A window is read
    from HARRIS_LC_LAG_DAYS before since_date, search filter and kept
    inspections alike, because the search field trails the inspection list
    (see the constant); a cut still holds the window at since_date itself.
    limit bounds the read: at most `limit` establishments are listed (newest
    first) and the newest `limit` of their inspections are read, and such a
    bounded read leaves no lag note. Only inspections whose item list was
    read (or whose score of 100 means there is none) ship.
    """
    global _HARRIS_FAILURES_LOGGED, _HARRIS_BLOCKED, _HARRIS_EXHAUSTED_STREAK, _HARRIS_LAST_ERROR
    log.info(f"Fetching Harris County data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    read_from = since
    if since and HARRIS_LC_LAG_DAYS:
        read_from = (datetime.strptime(since, '%Y-%m-%d') - timedelta(days=HARRIS_LC_LAG_DAYS)).strftime('%Y-%m-%d')
        log.info(f"  Harris County: reading from {read_from}, {HARRIS_LC_LAG_DAYS} days before {since} "
                 f"(lastCompletedInspectionDate trails the inspection list)")
    _HARRIS_FAILURES_LOGGED = 0
    with _HARRIS_LOCK:
        _HARRIS_STATS.clear()
        _HARRIS_BLOCKED, _HARRIS_EXHAUSTED_STREAK, _HARRIS_LAST_ERROR = False, 0, ''
    started = time.time()
    local = threading.local()

    def get_session():
        if not hasattr(local, 's'):
            local.s = requests.Session()
        return local.s

    def cut(status, detail):
        if since:
            # Nothing shipped is complete enough to advance the window: the
            # next run starts again at `since`.
            _hold_window_start(HARRIS_SOURCE, since, counted=False, key='window')
        _set_group_status(HARRIS_GROUP, status, detail)
        log.warning(f"Harris County: {status} — {detail}")
        return []

    # Phase 1: the establishments inspected in the window.
    skipped_est = defaultdict(int)
    try:
        listed = _harris_list_establishments(get_session(), read_from, limit)
    except _HarrisCut as c:
        return cut(c.status, c.detail)
    truncated = bool(limit) and len(listed) >= limit      # a bounded read never saw the end
    ests = [e for e in listed if _harris_in_scope(e, skipped_est)]
    if skipped_est:
        log.info("  Harris County: skipped " + ', '.join(
            f"{n} {why}" for why, n in sorted(skipped_est.items(), key=lambda kv: -kv[1]))
            + " establishments (not public food businesses)")

    # Phase 2: each establishment's inspection list. A breaker trip or the
    # deadline stops the phase and holds the window; a list that could not
    # be read (through every retry, or a 404/410 for an establishment
    # deactivated since the search) is that establishment's problem and is
    # skipped and held on its own below, like any bad record.
    def list_inspections(est):
        did = est.get('documentId')
        if _past_deadline():
            return did, 'deadline'
        if _HARRIS_BLOCKED:
            return did, 'blocked'
        info = {}
        data = _harris_request(get_session(), HARRIS_INSPECTIONS_URL, params={'id': did}, info=info)
        time.sleep(HARRIS_DELAY)
        if isinstance(data, list):
            return did, data
        return did, _harris_outcome(info)

    lists, outcomes, unread = {}, defaultdict(int), {}      # unread: documentId -> 'gone' | 'failed'
    if ests:
        log.info(f"  Harris County: reading {len(ests)} inspection lists ({HARRIS_WORKERS} workers)...")
        with ThreadPoolExecutor(max_workers=HARRIS_WORKERS) as pool:
            for future in as_completed([pool.submit(list_inspections, e) for e in ests]):
                did, data = future.result()
                if isinstance(data, list):
                    lists[did] = data
                elif data in ('gone', 'failed'):
                    unread[did] = data
                else:
                    outcomes[data] += 1
    if outcomes.get('blocked'):
        return cut('blocked', f'HCPH stopped answering ({_HARRIS_LAST_ERROR}); '
                              f'{sum(outcomes.values())} of {len(ests)} inspection lists not read')
    if outcomes.get('deadline'):
        return cut('skipped', f'fetch deadline reached; {sum(outcomes.values())} of {len(ests)} '
                              f'inspection lists not read')
    # A list that could not be read is skipped and held on its own (key =
    # documentId): the next lookback re-reads it and FRESHNESS_HOLD_GIVE_UP_RUNS
    # tried-and-failed runs give it up, so one permit the server always 500s
    # cannot fail the whole county every week. Many such lists are an outage.
    failed = sorted(did for did, why in unread.items() if why == 'failed')
    if len(failed) > HARRIS_LIST_FAILURES_MAX:
        raise RuntimeError(f"Harris County: {len(failed)} of {len(ests)} inspection lists could not be read "
                           f"({_HARRIS_LAST_ERROR}); not shipping a partial window")
    if unread:
        by_id = {e.get('documentId'): e for e in ests}
        for did in unread:
            day = sane_inspection_date(by_id[did].get('lastCompletedInspectionDate')) or read_from
            _hold_freshness(HARRIS_SOURCE, day, counted=True, key=did)
        gone = len(unread) - len(failed)
        if gone:
            log.warning(f"  Harris County: {gone} establishments answered 404/410 to their inspection "
                        f"list (deactivated since the search?); skipped and held for a re-read")
        if failed:
            log.warning(f"  Harris County: {len(failed)} inspection lists could not be read through every "
                        f"retry ({_HARRIS_LAST_ERROR}): {', '.join(failed[:5])}; skipped and held for a re-read")
        ests = [e for e in ests if e.get('documentId') not in unread]

    # The lists must agree with the search that chose them: an establishment
    # listed for lastCompletedInspectionDate holds a row dated within
    # HARRIS_LIST_AGREE_DAYS of it. One that does not (an empty list, or one
    # ending weeks before the field) was not read, whatever the server said,
    # and is held on its own; when most of them contradict — the list
    # endpoint answering [] to everyone — nothing ships, so a green run can
    # never be a dead endpoint's empty answer.
    checked, contradicted = 0, []
    for est in ests:
        field = sane_inspection_date(est.get('lastCompletedInspectionDate'))
        if not field:
            continue
        checked += 1
        newest = max((sane_inspection_date(r.get('date')) or ''
                      for r in (lists.get(est.get('documentId')) or []) if isinstance(r, dict)), default='')
        floor = (datetime.strptime(field, '%Y-%m-%d') - timedelta(days=HARRIS_LIST_AGREE_DAYS)).strftime('%Y-%m-%d')
        if newest < floor:
            contradicted.append((field, est.get('documentId')))
    if checked >= HARRIS_MASS_FAILURE_MIN and len(contradicted) > checked / 2:
        raise RuntimeError(f"Harris County: {len(contradicted)} of {checked} inspection lists hold no inspection "
                           f"near the date the search listed the establishment for; the lists are not being "
                           f"read, not shipping")
    for field, did in contradicted:
        _hold_freshness(HARRIS_SOURCE, field, counted=True, key=did)
    if contradicted:
        log.warning(f"  Harris County: {len(contradicted)} inspection lists hold nothing within "
                    f"{HARRIS_LIST_AGREE_DAYS} days of the establishment's lastCompletedInspectionDate "
                    f"({', '.join(f'{did} field {field}' for field, did in contradicted[:5])}); held for a re-read")

    dropped = defaultdict(int)
    candidates, seen = [], set()
    for est in ests:
        for insp in _harris_select_inspections(lists.get(est.get('documentId')) or [], read_from, dropped):
            if insp['number'] in seen:
                continue
            seen.add(insp['number'])
            candidates.append((insp['date'], insp['number'], est, insp))
    if dropped:
        log.info("  Harris County: left out " + ', '.join(
            f"{n} {why}" for why, n in sorted(dropped.items(), key=lambda kv: -kv[1]))
            + " inspections (visit type, status or date)")
        for why in dropped:
            if why.startswith('unknown type'):
                log.warning(f"  Harris County: {dropped[why]} inspections of an {why} were dropped — "
                            f"decide whether that type should ship")
    candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)       # newest first
    if limit and len(candidates) > limit:
        candidates = candidates[:limit]
        truncated = True
    if not candidates:
        if not truncated:
            _note_source_lag(HARRIS_GROUP, _harris_newest(get_session()))
        log.info("Harris County: 0 inspections")
        return []

    # Phase 3: the OUT items of each inspection, oldest first so a cut drops
    # the newest tail. A score of 100 has no items (HARRIS_READ_CLEAN_ITEMS).
    def read_items(cand):
        date, number, est, insp = cand
        if insp.get('score') == 100 and not HARRIS_READ_CLEAN_ITEMS:
            return cand, []                 # nothing to read, so a cut never drops it
        if _past_deadline():
            return cand, 'deadline'
        if _HARRIS_BLOCKED:
            return cand, 'blocked'
        info = {}
        data = _harris_request(get_session(), HARRIS_VIOLATIONS_URL, params={'id': number}, info=info)
        time.sleep(HARRIS_DELAY)
        if isinstance(data, list):
            return cand, data
        return cand, _harris_outcome(info)

    items, failures = {}, defaultdict(int)
    log.info(f"  Harris County: reading the items of {len(candidates)} inspections "
             f"({HARRIS_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=HARRIS_WORKERS) as pool:
        ordered = sorted(candidates, key=lambda t: (t[0], t[1]))
        for future in as_completed([pool.submit(read_items, c) for c in ordered]):
            cand, data = future.result()
            if isinstance(data, list):
                items[cand[1]] = data
            else:
                failures[data] += 1
                # Dropped, never shipped clean; the hold makes the next
                # lookback re-read it (a tried-and-failed read — or a 404 —
                # counts toward giving the record up, a breaker/deadline
                # drop never does).
                _hold_freshness(HARRIS_SOURCE, cand[0], counted=(data in ('failed', 'gone')), key=cand[1])
    if failures.get('blocked'):
        _set_group_status(HARRIS_GROUP, 'blocked',
                          f"HCPH stopped answering ({_HARRIS_LAST_ERROR}); "
                          f"{sum(failures.values())} inspections not read")
    elif failures.get('deadline'):
        _set_group_status(HARRIS_GROUP, 'skipped',
                          f"fetch deadline reached; {sum(failures.values())} inspections not read")
    if failures:
        log.warning("  Harris County: dropped " + ', '.join(
            f"{n} ({why})" for why, n in sorted(failures.items())) +
            " inspections whose items could not be read")

    counts = defaultdict(int)
    results, unread_items, scored_read = [], [], 0
    for date, number, est, insp in candidates:
        if number not in items:
            continue
        score = _harris_score(insp)
        if score is not None and 0 < score < 100:
            scored_read += 1
            if _harris_items_unread(insp, items[number]):
                unread_items.append((date, number))
                continue
        rec = _harris_record(est, insp, items[number], counts)
        if rec:
            results.append(rec)
    # A score below 100 with no OUT item read is an item list that was not
    # read (see _harris_items_unread): dropped and held like an unreadable
    # one, never shipped as clean; when most scored inspections look like
    # that, the item endpoint is not answering and nothing ships.
    if scored_read >= HARRIS_MASS_FAILURE_MIN and len(unread_items) > scored_read / 2:
        raise RuntimeError(f"Harris County: {len(unread_items)} of {scored_read} inspections scored below 100 "
                           f"came back with no OUT item; the item lists are not being read, not shipping")
    for date, number in unread_items:
        _hold_freshness(HARRIS_SOURCE, date, counted=True, key=number)
    if unread_items:
        log.warning(f"  Harris County: {len(unread_items)} inspections scored below 100 came back with no "
                    f"OUT item; dropped and held for a re-read (an item list that reads as clean was not read)")
    if counts.get('unscored'):
        log.info(f"  Harris County: {counts['unscored']} unscored visits without items left out")
    if counts.get('score != 100 - demerits'):
        log.warning(f"  Harris County: {counts['score != 100 - demerits']} inspections whose score "
                    f"is not 100 minus the items' demerits (the county's score was kept)")
    if counts.get('entered late'):
        log.info(f"  Harris County: {counts['entered late']} inspections were entered more than "
                 f"{HARRIS_LATE_ENTRY_DAYS} days after their date")
    # Newest first; within a day the worst visit first (a closure, then the
    # lowest score, then the most items) so same-day picks read the same in
    # either output path.
    results.sort(key=lambda r: (r['inspection_date'], r['results'] == 'Closure required',
                                -r['risk_score'], r['total_violations'], r['source_id']),
                 reverse=True)
    if not truncated:
        newest = _harris_newest(get_session()) or max((r['inspection_date'] for r in results), default='')
        _note_source_lag(HARRIS_GROUP, newest)
    log.info(f"Harris County: {len(results)} inspections from {len(ests)} establishments "
             f"({_HARRIS_STATS.get('requests', 0)} requests, {time.time() - started:.0f}s)")
    return results


# ─── HEDGEROW PORTALS: PIMA COUNTY AZ, LUBBOCK TX, MESQUITE TX ───────────────
# Hedgerow Software's "Hedgehog" public-disclosure portal: an Angular SPA over
# an undocumented, anonymous JSON API at <host>/api/v0/portal/ (request shapes
# read from the SPA bundles, probed live 2026-11-02/03). Three agencies run it
# and one client serves all three:
#   POST disclosure/program/facilities                the program's facilities
#        (facilityName, siteAddress as ONE string, latitude/longitude,
#        facilityType, lastInspectionDate and the newest inspection's
#        hazardScore/hazardRating — each portal hides some of these)
#   GET  disclosure/program/{P}/facility/{F}/inspectionDetails/   every
#        published inspection of one facility (id, inspectionDate as a local
#        timestamp, inspectionType, inspectionNumber, hazardScore, hazardRating)
#   GET  disclosure/program/inspectionResults/{I}     one inspection's
#        checklist: items with a result, and for each cited item cannedComments[]
#        carrying the agency's severity (Priority / Priority Foundation / Core),
#        the inspector's observation and a location
# No endpoint filters by date, so the window is applied client side: a week
# costs one list read plus one GET per facility inspected in the window and
# one per inspection. Timestamps carry the local offset
# ('2026-10-16T23:45:39.79-07:00'): the first ten characters are the
# inspection day — converting to UTC would move evening inspections a day.
#
# Pima County AZ ('Pima County' / 'Pima County Health Department'): the whole
#   list in one response (5,177 rows, 5 MB) and ONLY when sorted by id — the
#   SPA's own facilityName sort times out server-side (500 after ~31 s, 6/6
#   tries). No numeric score anywhere (hazardScore null on every row and
#   inspection): the risk score is computed from the citations' severities
#   (100 - 5P - 2PF - C) and the county's categorical rating goes to results.
#   The portal publishes ~16 days behind (newest 2026-10-17 on 2026-11-02), so
#   the newest lastInspectionDate is left as the source lag.
# Lubbock TX ('Lubbock' / 'Lubbock Environmental Health'): paged list
#   (pageSize 500) sorted lastInspectionDate desc, read while a page's last
#   row is still in the window; official 0-100 hazardScore per inspection.
# Mesquite TX ('Mesquite' / 'Mesquite Environmental Health', DFW; public
#   portal https://mesquite.hedgerowsoftware.com/, whose API proxies to the
#   app server msqt-app-prod.hedgerowsoftware.com): the whole list (689 rows)
#   with facilityType and lastInspectionDate hidden, and the
#   server does not honour a sort on the hidden date (verified 2026-11-03:
#   newest dates rise and fall down the "sorted" list), so EVERY facility's
#   inspection list is read each run (~690 GETs, a few minutes). Official
#   0-100 hazardScore; no observation text (item titles plus the short rule
#   names are the violation text); 39% of rows carry no coordinates and are
#   geocoded by main().
#
# Scope: pools, body art, campgrounds etc. are separate programs and never
# requested. Within the food program, temporary-event booths (Lubbock's
# 'TF15 Temporary Food', 729 rows for one-off events), schools where the type
# says so, and mobile units (type says Mobile / Push Cart / Vehicle) are
# dropped: a mobile unit's siteAddress is its commissary, and 84 Pima trucks
# share one — pinning them there would put 84 "restaurants" on a kitchen.
# Mesquite hides the type, so all of its rows ship.
#
# Fail-closed rules (review 2026-11-03):
# - A 400/404/410 is that one record's problem (dropped, never held, as
#   SNHD's permits are); EVERY other failure — 401/403 (a block), 408, 429,
#   5xx, a timeout, a non-JSON body — is the portal's: retried, then the
#   record is dropped AND held so the next window re-reads it.
#   HH_BREAKER_TRIP requests in a row that exhaust their retries trip a
#   per-portal breaker: every further request of the run fails at once, the
#   unread tail is held uncounted and the group reports 'blocked' (a hanging
#   portal could otherwise burn the whole fetch deadline: Mesquite issues
#   ~690 GETs a run and one failing request costs up to ~195 s). Scattered
#   failures beyond HH_MASS_FAILURE_MIN and HH_MASS_FAILURE_FRACTION of the
#   reads report 'error'.
# - A checklist without a cited item is CLEAN only when the inspection row
#   agrees. Pima's rows carry hasInfractions (True on every cited visit seen
#   live, False on every clean one; an Enforcement 'Fail' whose 62 items
#   were all 'No Answer Selected' was observed and would otherwise compute
#   to a 100), Lubbock's carry criticalInfractionCount (it counts every
#   citation, Core included). Lubbock's and Mesquite's clean checklists ARE
#   empty lists (verified live: every score-100 visit answered []), so an
#   empty list is unread only where the row says there were infractions;
#   Mesquite's rows carry neither flag and its score is the county's own.
# - Lubbock's paging trusts nothing it can check: the echoed pageSize must
#   be HH_PAGE (a silent clamp would end the read after one short page) and
#   every page must be sorted newest-inspected first with never-inspected
#   rows last and continue the page before (the same vendor's Mesquite
#   server ignores that sort), or the list raises and the window ships
#   nothing.
# - Pima publishes each report as it is finalised, ~16 days after the
#   inspection and spread over several days (2026-11-03: Thursday 10-16
#   showed 15 facilities against 26-33 on the other Thursdays, Fridays 1-2),
#   so a window that starts the day before the last shipped inspection
#   misses the stragglers. 'lag_margin_days' widens every dated window that
#   far; holds stay at the start the caller asked for, so the margin never
#   creeps back. The same margin lets a weekly run with nothing recorded
#   (an 8-day window, all of it newer than anything the portal has
#   published) read its first batch instead of shipping a green 'empty'
#   forever; a window that still starts after the portal's newest row
#   reports 'error' and says how to seed the group.
HH_HEADERS = {'User-Agent': CHROME_UA, 'Accept': 'application/json, text/plain, */*'}
HH_POST_HEADERS = dict(HH_HEADERS, **{'Content-Type': 'application/json'})
HH_TIMEOUT = 60              # one detail GET (measured 0.2-2.6 s)
HH_LIST_TIMEOUT = 180        # the facility list (Pima's 5 MB takes ~5 s)
HH_RETRIES = 3               # attempts per request
HH_RETRY_WAIT = 5            # seconds, times the attempt number
HH_DELAY = 0.3               # pause after every detail GET, per worker
HH_WORKERS = 3               # concurrent detail readers
HH_PAGE = 500                # Lubbock's page size (3000 was honoured; 500 keeps a page ~0.4 MB)
HH_MAX_PAGES = 200           # paging guard
HH_RULE_MAX = 90             # a cited rule's short name may stand in for a missing observation
HH_VIOLATION_RESULTS = frozenset({'Out of Compliance', 'Corrected During Inspection'})
HH_SEVERITY = {'priority': 'priority', 'priority foundation': 'priority_foundation', 'core': 'core'}
HH_MOBILE_TYPE_RE = re.compile(r'mobile|push ?cart|vehicle', re.I)
HH_SKIP_TYPES = frozenset({'TF15 Temporary Food', 'School'})   # Lubbock's vocabulary
HH_PERMANENT_STATUSES = frozenset({400, 404, 410})   # that record's own problem; every other status is the portal's
HH_BREAKER_TRIP = 3          # requests in a row that exhaust their retries -> the portal is blocked this run
HH_MASS_FAILURE_MIN = 10     # unread (held) lists/checklists that report the group 'error' when they are also
HH_MASS_FAILURE_FRACTION = 0.1   # ...more than this share of the reads (fetch failures: 5xx, timeouts, blocks)
HH_MASS_UNPUBLISHED_FRACTION = 0.5   # ...or, for checklists the county never filled in, more than this share
#                                      of the checklists read (5 of the 25 oldest Pima visits, 2019-2022, were
#                                      unpublished; the 2026-09/10 window 1.6%; a vendor renaming the result
#                                      field would make it 100%)
# A checklist item's possible answers; an item outside this vocabulary (a
# renamed field, a new result) is neither a citation nor a clean answer.
HH_ANSWERED_RESULTS = HH_VIOLATION_RESULTS | {'In Compliance', 'Not Applicable', 'Not Observed'}
HH_PORTALS = {
    'pima': {
        'group': 'Pima County', 'source': 'Pima County Health Department', 'prefix': 'pima',
        'base': 'https://healthinspect.pima.gov/portal/api/v0/portal/',
        'site': 'https://healthinspect.pima.gov/portal/',
        'program': 'c1df4447-c948-4d39-a649-b8855a33b857',
        'state': 'AZ', 'default_city': 'Tucson', 'metro': 'Tucson',
        'history_start': '2019-10-01',      # the portal launched in Oct 2019; oldest row seen 2019-12-05
        'list': 'whole', 'list_sort': 'id', 'list_min_rows': 4000,   # 5,177 rows on 2026-11-02
        'dated_list': True, 'official_score': False, 'results': 'rating',
        'title_caps': True,                 # 2,229 of 5,177 names (and their streets) are ALL CAPS
        'lag_margin_days': 21,              # reports publish ~16 days late, spread over days (section comment)
    },
    'lubbock': {
        'group': 'Lubbock', 'source': 'Lubbock Environmental Health', 'prefix': 'lbk',
        'base': 'https://clhd-portal.hedgerowsoftware.com/api/v0/portal/',
        'site': 'https://clhd-portal.hedgerowsoftware.com/',
        'program': 'd88b111d-edad-49f8-83a8-8c1d7931f99b',
        'state': 'TX', 'default_city': 'Lubbock', 'metro': '',
        'history_start': '2023-11-01',      # oldest published inspection 2023-11-11
        'list': 'paged', 'list_sort': 'lastInspectionDate', 'list_min_rows': 1,
        'dated_list': True, 'official_score': True, 'results': 'grade',
        'title_caps': False,                # mixed case as published; '(OXXO) DK #401' is a brand
        'lag_margin_days': 0,               # an inspection is on the portal the next day
    },
    'mesquite': {
        'group': 'Mesquite', 'source': 'Mesquite Environmental Health', 'prefix': 'mesq',
        'base': 'https://mesquite.hedgerowsoftware.com/api/v0/portal/',
        'site': 'https://mesquite.hedgerowsoftware.com/',
        'program': '6ee6f958-ea40-45cb-bb87-ba2e06bd1257',
        'state': 'TX', 'default_city': 'Mesquite', 'metro': 'DFW',
        'history_start': '2020-01-01',      # no date on the list; oldest inspection seen 2025-11-04
        'list': 'whole', 'list_sort': 'id', 'list_min_rows': 400,     # 689 rows on 2026-11-03
        'dated_list': False, 'official_score': True, 'results': 'none',
        'title_caps': False,
        'lag_margin_days': 0,               # no dates on the list; every inspection list is read anyway
    },
}
# Pima's inspections carry a categorical rating. Graded visits ship with the
# rating in results; a failed re-inspection, an imminent-health-hazard
# closure and a probation cap the computed score below 70 (the county's own
# verdict: a 'Fail' re-inspection re-checks only the open items, so its two
# citations would otherwise compute to a 97). Visits without a checklist
# verdict (site visits, emergency responses, 'No rating' and the vocabulary
# of other programs) never ship — a site visit has no citations and would
# read as a clean 100.
# The county's own ordering, from its portal's colours: Excellent and Pass
# green, Good light blue, Satisfactory orange, Needs Improvement purple,
# Probation / Fail / IHH red. A 'Needs Improvement' is up to four priority
# or priority-foundation items NOT corrected during the inspection (the
# county's definition, KOLD 2020-01-31) and the step before Probation: a
# re-inspection verdict like Orange County's REINSPECTION (75) and Santa
# Clara's yellow placard, so it caps the computed score in the C band (its
# citations alone computed to 86-93, an A, on 2026-11-03), and a
# 'Satisfactory' can never show as an A. A cap never raises a score (a
# Probation whose citations compute to 62 stays 62). 'Good' and 'Excellent'
# are not banded: the citations decide between A and B.
PIMA_SHIP_RATINGS = frozenset({
    'Excellent', 'Good', 'Satisfactory', 'Needs Improvement', 'Probation', 'Fail',
    'Pass', 'Educational', 'Complaint Open', 'Complaint Closed', 'IHH',
})
PIMA_RATING_RISK_CAP = {'Fail': 55, 'IHH': 55, 'Probation': 65, 'Needs Improvement': 79, 'Satisfactory': 89}
# Lubbock publishes the letter only for a facility's newest inspection (per
# inspection it is the literal 'INFO_REDACTED'); its bounds were verified on
# all 2,044 rated facilities (A 90-100, B 80-89, C 70-79, F below).
LBK_GRADE_BOUNDS = (('A', 90), ('B', 80), ('C', 70))
_HH_STATE_NAMES = {'arizona': 'AZ', 'texas': 'TX', 'new mexico': 'NM'}
_HH_STATE_RE = r'[A-Za-z]{2}|Arizona|Texas|New Mexico'
# 'STREET[, Suite], City ST 85705' (also 'City Arizona 85705', zip+4)
_HH_ADDR_RE = re.compile(
    r'^(?P<street>.*?),\s*(?P<city>[^,]+?)\s+(?P<state>' + _HH_STATE_RE + r')\s+(?P<zip>\d{5})(?:-?\d{4})?\s*$', re.I)
# 'STREET, City ST' (zip missing)
_HH_ADDR_NOZIP_RE = re.compile(
    r'^(?P<street>.*?),\s*(?P<city>[^,]+?)\s+(?P<state>' + _HH_STATE_RE + r')\s*$', re.I)
# 'STREET, City 794232249 TX' (zip before the state, zip+4 run together)
_HH_ADDR_ZIP_STATE_RE = re.compile(
    r'^(?P<street>.*?),\s*(?P<city>[^,]+?)\s+(?P<zip>\d{5})\d{0,4}\s+(?P<state>[A-Za-z]{2})\s*$')
# 'STREET City, ST 79416' (the comma after the city instead of before it)
_HH_ADDR_CITY_IN_STREET_RE = re.compile(
    r'^(?P<street>.+?)\s+(?P<city>[A-Za-z.]+),\s*(?P<state>[A-Za-z]{2})\s+(?P<zip>\d{5})(?:-?\d{4})?\s*$')
# 'STREET, TX 75149' (no city at all)
_HH_ADDR_STATE_ZIP_RE = re.compile(
    r'^(?P<street>.*?),\s*(?P<state>' + _HH_STATE_RE + r')\s+(?P<zip>\d{5})(?:-?\d{4})?\s*$', re.I)
# 'STREET, Tucsonaz 85713' (state run into the city, or missing)
_HH_ADDR_NOSTATE_RE = re.compile(
    r'^(?P<street>.*?),\s*(?P<city>[^,]+?)\s+(?P<zip>\d{5})(?:-?\d{4})?\s*$')
# A word before the comma that is a street suffix is not a city ('1700 Rodeo Dr, TX 75149').
_HH_STREET_SUFFIXES = frozenset({
    'ave', 'avenue', 'blvd', 'boulevard', 'cir', 'circle', 'ct', 'court', 'dr', 'drive', 'fwy', 'freeway',
    'hwy', 'highway', 'ln', 'lane', 'loop', 'pkwy', 'parkway', 'pl', 'place', 'rd', 'road', 'st', 'street',
    'trl', 'trail', 'way', 'expy', 'expressway', 'ter', 'terrace', 'plz', 'plaza', 'sq', 'square',
})
_HH_ORDINAL_RE = re.compile(r'(\d)(St|Nd|Rd|Th)\b')
_HH_PLACEHOLDER_RE = re.compile(r'\s*@?\s*_{2,}\s*(?:PPM|°F|F\b)?', re.I)
# The Texas form's pest criterion is titled 'No evidence of Insect
# Contamination, Rodent, Other Animals', Pima's 'Insects, rodents, and
# animals not present'. The Texas title alone trips the text-forced F (an
# evidence word before a pest word; measured 2026-11-03: 10 of Lubbock's 72
# window records graded F for fly tape, door gaps and ceiling tiles, 8 of
# Mesquite's 136 for 'Outer openings'), and EITHER title's 'rodent' matches
# the regex across the '|||' line join when the line before ends in an
# evidence word ('... soap not found|||Insects, rodents, ...') and defeats
# Jev's named-vermin guard. Both therefore ship under one neutral heading,
# 'Pests and animals: <observation>' (not 'Pest control', which the regex
# summariser reads as "No pest control records"): a sticky fly trap stays a
# Core line, 'Observed live roaches' still forces an F from the observation.
_HH_PEST_TITLE_RE = re.compile(
    r'^(?:(?:no\s+)?evidence\s+of\s+insect\s+contamination,?\s+rodent[/,]?\s*(?:and\s+)?other\s+animals'
    r'|insects,?\s+rodents,?\s+(?:and\s+)?animals\s+(?:are\s+)?not\s+present)$', re.I)
HH_PEST_TITLE = 'Pests and animals'
_HH_CODE_PREFIX_RE = re.compile(r'^\s*(?:§\s*)?[\d][\w.()§-]*\s*(?:-|–|:)\s*')
_HH_REQUESTS = defaultdict(int)       # portal slug -> requests this run (for the log)
_HH_STREAK = defaultdict(int)         # portal slug -> requests in a row that exhausted their retries
_HH_BLOCKED = {}                      # portal slug -> reason, once the breaker tripped this run
_HH_REQUESTS_LOCK = threading.Lock()


class _HhError(Exception):
    """A Hedgehog request that could not be completed. `permanent` is True
    for a 400/404/410 (that record is gone from the portal: dropped, never
    held); `blocked` is True when the request was not made because the
    portal's breaker had tripped (held, not counted). Otherwise the failure
    is transient — a timeout, a reset, 401/403/408/429, 5xx, a non-JSON
    body — and the record is held and re-read next run."""

    def __init__(self, msg, permanent=False, blocked=False):
        super().__init__(msg)
        self.permanent = permanent
        self.blocked = blocked


def _hh_blocked(slug):
    """Why the portal's breaker tripped this run, or '' while it has not."""
    with _HH_REQUESTS_LOCK:
        return _HH_BLOCKED.get(slug, '')


def _hh_json(slug, path, body=None, timeout=HH_TIMEOUT):
    """GET (or POST `body` as JSON) one Hedgehog endpoint and return the
    decoded JSON. Transient failures are retried HH_RETRIES times with
    backoff, never past the fetch deadline; a 400/404/410 is raised at once.
    HH_BREAKER_TRIP requests in a row that exhaust their retries trip the
    portal's breaker, and every later request of the run fails at once
    (`blocked`); a success resets the streak, a retry the deadline cut
    short and a permanent 4xx count for nothing."""
    group = HH_PORTALS[slug]['group']
    reason = _hh_blocked(slug)
    if reason:
        raise _HhError(f'portal breaker tripped: {reason}', blocked=True)
    url = HH_PORTALS[slug]['base'] + path
    last, exhausted = None, False
    for attempt in range(1, HH_RETRIES + 1):
        with _HH_REQUESTS_LOCK:
            _HH_REQUESTS[slug] += 1
        try:
            if body is None:
                r = requests.get(url, headers=HH_HEADERS, timeout=timeout)
            else:
                r = requests.post(url, json=body, headers=HH_POST_HEADERS, timeout=timeout)
        except requests.RequestException as e:
            last = _HhError(f'{type(e).__name__}: {str(e)[:120]}')
        else:
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    last = _HhError(f'non-JSON body {r.text[:60]!r}')
                else:
                    with _HH_REQUESTS_LOCK:
                        _HH_STREAK[slug] = 0
                    return data
            elif r.status_code in HH_PERMANENT_STATUSES:
                raise _HhError(f'HTTP {r.status_code}', permanent=True)
            else:
                # 401/403 (a block), 408, 429, 5xx: the portal's problem, not the record's
                last = _HhError(f'HTTP {r.status_code}')
        if attempt == HH_RETRIES:
            exhausted = True
            break
        if _past_deadline():
            break
        wait = HH_RETRY_WAIT * attempt
        log.warning(f"  {group}: {path[:90]} failed ({last}) — retry {attempt}/{HH_RETRIES - 1} in {wait}s")
        time.sleep(wait)
    if exhausted:
        with _HH_REQUESTS_LOCK:
            _HH_STREAK[slug] += 1
            tripped = _HH_STREAK[slug] >= HH_BREAKER_TRIP and slug not in _HH_BLOCKED
            if tripped:
                _HH_BLOCKED[slug] = f'{_HH_STREAK[slug]} requests in a row failed (last: {last})'
        if tripped:
            log.error(f"  {group}: {_HH_BLOCKED[slug]} — treating the portal as blocked; the run's "
                      f"remaining requests are skipped and their records held")
    raise last


def _hh_clean(s):
    """Collapse whitespace, drop the form's fill-in placeholders ('@____ PPM')."""
    s = _HH_PLACEHOLDER_RE.sub(' ', str(s or ''))
    return ' '.join(s.split())


def _hh_title(s):
    """Whitespace-collapsed; .title() only for an ALL-CAPS value (Pima prints
    43% of names and streets in capitals), keeping possessives and ordinals
    ("JOE'S", '1ST AVE' -> "Joe's", '1st Ave')."""
    s = ' '.join(str(s or '').split())
    if s.isupper():
        s = re.sub(r"'S\b", "'s", s.title())
        s = _HH_ORDINAL_RE.sub(lambda m: m.group(1) + m.group(2).lower(), s)
    return s


def _hh_case(s, cfg):
    """A name or street as the portal prints it: whitespace collapsed, and
    for a portal that prints in capitals (Pima) ALL CAPS -> title case."""
    return _hh_title(s) if cfg.get('title_caps') else ' '.join(str(s or '').split())


def _hh_coord(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f and abs(f) <= 180 else None


def _hh_split_address(addr, cfg):
    """One siteAddress string -> (street, city, state, zip) or None when there
    is no street. 'STREET[, Suite], City ST 85705' is the rule (5,163 of
    5,177 Pima rows, 2,306 of 2,323 Lubbock rows); the deviants seen are
    'City Arizona 85705', 'Tucsonaz 85713', a missing zip, 'City 794232249
    TX' and 'STREET City, TX 79416'. A city that cannot be read falls back
    to the portal's default city with an empty zip."""
    addr = ' '.join(str(addr or '').split())
    if not addr:
        return None
    m = _HH_ADDR_RE.match(addr) or _HH_ADDR_ZIP_STATE_RE.match(addr)
    if not m:
        m = _HH_ADDR_CITY_IN_STREET_RE.match(addr)
        if m and (len(m.group('city')) < 4 or m.group('city').lower() in _HH_STREET_SUFFIXES):
            m = _HH_ADDR_STATE_ZIP_RE.match(addr)
    if not m:
        m = _HH_ADDR_NOSTATE_RE.match(addr) or _HH_ADDR_NOZIP_RE.match(addr)
    if not m:
        return _hh_case(addr.strip(' ,'), cfg), cfg['default_city'], cfg['state'], ''
    g = m.groupdict()
    state = _HH_STATE_NAMES.get((g.get('state') or cfg['state']).lower(), (g.get('state') or cfg['state']).upper())
    city = ' '.join((g.get('city') or '').split())
    if len(city) > 2 and city.lower().endswith(state.lower()) and not city[-3].isspace():
        city = city[:-2]                 # 'Tucsonaz'
    if len(city) == 2 and city.upper() in set(_HH_STATE_NAMES.values()) | {cfg['state']}:
        state, city = city.upper(), ''   # '3220 Gus Thomasson Mesquite 101, TX 75150': no city given
    street = _hh_case(g['street'].strip(' ,'), cfg)
    if not street:
        return None
    return street, (city.title() or cfg['default_city']), state, (g.get('zip') or '')[:5]


def _hh_facility(cfg, row, skipped):
    """Facility row -> the fields a record needs, or None for a row outside
    scope (counted in `skipped` by reason)."""
    if not isinstance(row, dict):
        skipped['malformed row'] += 1
        return None
    fid = str(row.get('id') or '').strip()
    name = _hh_case(row.get('facilityName'), cfg)
    ftype = ' '.join(str(row.get('facilityType') or '').split())
    if not fid or not name:
        skipped['no id or name'] += 1
        return None
    if ftype in HH_SKIP_TYPES:
        skipped[ftype] += 1
        return None
    if HH_MOBILE_TYPE_RE.search(ftype):
        skipped['mobile unit'] += 1
        return None
    parts = _hh_split_address(row.get('siteAddress'), cfg)
    if parts is None:
        skipped['no address'] += 1
        return None
    street, city, state, zip5 = parts
    if state != cfg['state']:
        skipped['outside ' + cfg['state']] += 1
        return None
    return {
        'id': fid, 'name': name, 'street': street, 'city': city, 'zip': zip5,
        'lat': _hh_coord(row.get('latitude')), 'lng': _hh_coord(row.get('longitude')),
        'last': str(row.get('lastInspectionDate') or '')[:10],
        'rating': ' '.join(str(row.get('hazardRating') or '').split()),
    }


def _hh_list_body(cfg, page=None):
    body = {'sort': [{'field': cfg['list_sort'], 'order': 'desc' if cfg['list'] == 'paged' else 'asc'}],
            'disclosureProgramId': cfg['program'], 'fields': ['facilityName']}
    if page is not None:
        body.update({'pageNumber': page, 'pageSize': HH_PAGE, 'criteria': '', 'fields': [], 'filters': []})
    return body


def _hh_facility_rows(slug, since):
    """Every facility row the run needs. Pima and Mesquite answer with the
    whole program in one response (paging is ignored); Lubbock pages, sorted
    newest-inspected first, and the read stops at the first page whose last
    row was inspected before `since` (never-inspected rows sort last). Any
    failure, a bad shape or an implausibly short whole list raises: the list
    is the completeness check, so a partial one must ship nothing."""
    cfg = HH_PORTALS[slug]
    rows, page, total, prev_last = [], 0, None, ''
    while True:
        if page and _past_deadline():
            raise RuntimeError(f"{cfg['group']}: fetch deadline reached while listing facilities "
                               f"(page {page})")
        try:
            payload = _hh_json(slug, 'disclosure/program/facilities',
                               _hh_list_body(cfg, page if cfg['list'] == 'paged' else None),
                               timeout=HH_LIST_TIMEOUT)
        except _HhError as e:
            raise RuntimeError(f"{cfg['group']}: facility list page {page} failed: {e}") from e
        page_rows = payload.get('result') if isinstance(payload, dict) else None
        if not isinstance(page_rows, list):
            raise RuntimeError(f"{cfg['group']}: facility list has an unexpected shape: "
                               f"{str(payload)[:120]}")
        try:
            total = int(payload.get('totalNumberOfRecords')) or total
        except (TypeError, ValueError):
            pass
        rows.extend(page_rows)
        if cfg['list'] != 'paged':
            if len(rows) < cfg['list_min_rows']:
                raise RuntimeError(f"{cfg['group']}: facility list has only {len(rows)} rows "
                                   f"(expected at least {cfg['list_min_rows']}); not trusting it")
            return rows
        # Two server behaviours the paged read depends on, both checked
        # against the response: the page size (a silent clamp would end the
        # read after one short page) and the sort (newest-inspected first,
        # never-inspected rows last, each page continuing the one before —
        # the same vendor's Mesquite server ignores a sort on this field).
        # A list that fails either is not trusted: nothing ships.
        try:
            echo = int(payload.get('pageSize') or 0)
        except (TypeError, ValueError):
            echo = 0
        if echo and echo != HH_PAGE:
            raise RuntimeError(f"{cfg['group']}: facility list page {page} came back with pageSize {echo}, "
                               f"not the {HH_PAGE} asked for; not trusting the paging")
        dates = [str(r.get('lastInspectionDate') or '')[:10] if isinstance(r, dict) else '' for r in page_rows]
        dated = [d for d in dates if d]
        if (dated != sorted(dated, reverse=True) or dates != dated + [''] * (len(dates) - len(dated))
                or (prev_last and dated and dated[0] > prev_last)):
            raise RuntimeError(f"{cfg['group']}: facility list page {page} is not sorted newest-inspected "
                               f"first; not trusting the paging")
        prev_last = dated[-1] if dated else prev_last
        last = dates[-1] if dates else ''
        log.info(f"  {cfg['group']}: facility page {page} — {len(page_rows)} rows"
                 f"{f' of {total}' if total else ''}, last inspected {last or 'never'}")
        # The read ends at an empty page, the echoed total, the first row
        # inspected before the window, or a short page whose size the
        # server confirmed.
        if (not page_rows or last < since or (total and len(rows) >= total)
                or (len(page_rows) < HH_PAGE and echo == HH_PAGE)):
            return rows
        page += 1
        if page >= HH_MAX_PAGES:
            raise RuntimeError(f"{cfg['group']}: facility list did not end after {page} pages")


def _hh_violations(items):
    """Checklist -> [[category, severity, text], ...]: one line per citation
    (cannedComment) of each cited item ('Out of Compliance' / 'Corrected
    During Inspection'). The text is the item's title plus the inspector's
    observation and location; the Food Code paragraph the citation quotes
    ("...shall be maintained free of insects, rodents...") is never shipped,
    only its short rule name when there is no observation (Mesquite). The
    severity is the agency's own; a cited item without a citation keeps the
    classifier's. Raises ValueError for anything but a list."""
    if not isinstance(items, list):
        raise ValueError(f'unexpected inspectionResults shape: {str(items)[:80]}')
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if ' '.join(str(item.get('result') or '').split()) not in HH_VIOLATION_RESULTS:
            continue
        title = _hh_clean(item.get('description')).rstrip(' .:;')
        if _HH_PEST_TITLE_RE.match(title):
            title = HH_PEST_TITLE
        comments = item.get('cannedComments')
        if not isinstance(comments, list) or not comments:
            comments = [{}]
        for c in comments:
            c = c if isinstance(c, dict) else {}
            obs = _hh_clean(c.get('observationComment')).rstrip(' .')
            loc = _hh_clean(c.get('locationComment')).rstrip(' .')
            text = title
            if obs and obs.lower() != title.lower():
                text = f'{title}: {obs}' if title else obs
            elif not obs:
                rule = _HH_CODE_PREFIX_RE.sub('', _hh_clean(c.get('description'))).rstrip(' .')
                if rule and len(rule) <= HH_RULE_MAX and rule.lower() != title.lower():
                    text = f'{title}: {rule}' if title else rule
            if loc:
                text = f'{text} ({loc})' if text else loc
            if not text:
                continue
            cat, sev, _ = classify_violation(text)
            stated = HH_SEVERITY.get(_hh_clean(c.get('severity')).lower())
            out.append([cat, stated or sev, text[:500]])
    return out


def _hh_clean_is_believable(cfg, cand, items):
    """Whether a checklist with no cited item may ship as a clean visit. The
    inspection row is the cross-check: Pima's hasInfractions flag, Lubbock's
    criticalInfractionCount (every citation, Core included). Without either
    (Mesquite) the empty list IS the county's clean checklist and the score
    is the county's own; Pima's 62-item form always carries answered items,
    so a Pima checklist with none ([], all 'No Answer Selected', a renamed
    result field) is unread, never a clean 100."""
    if cand.get('infractions') is not None:
        return not cand['infractions']
    if cand.get('count') is not None:
        return cand['count'] == 0
    if cfg['official_score']:
        return True
    return any(' '.join(str(i.get('result') or '').split()) in HH_ANSWERED_RESULTS
               for i in items if isinstance(i, dict))


def _lbk_grade(score):
    for letter, floor in LBK_GRADE_BOUNDS:
        if score >= floor:
            return letter
    return 'F'


def _hh_record(cfg, fac, cand, violations):
    """One DineScores record, or None for an unscored visit on an official-
    score portal (a published score of 0/null: the county scored nothing, and
    a 5/2/1 count over its citations would grade it on another scale than
    every other record of the source — the Texas form deducts 3/2/1)."""
    pv = sum(1 for v in violations if v[1] == 'priority')
    pfv = sum(1 for v in violations if v[1] == 'priority_foundation')
    cv = len(violations) - pv - pfv
    score = cand['score']
    rating = cand['rating']
    if cfg['official_score']:
        if score is None or score <= 0:
            return None
        original, risk = score, max(0, min(100, score))
    else:
        original = None
        risk, pv, pfv, cv = calc_risk_score(violations)
        cap = PIMA_RATING_RISK_CAP.get(rating)
        if cap is not None:
            risk = min(risk, cap)        # the county's verdict is never contradicted upward
    if cfg['results'] == 'rating':
        results = rating
    elif cfg['results'] == 'grade' and original is not None:
        results = f'Grade {_lbk_grade(original)}'
    else:
        results = ''
    return {
        'name': fac['name'],
        'address': fac['street'],
        'city': fac['city'],
        'state': cfg['state'],
        'zip': fac['zip'],
        'latitude': fac['lat'],
        'longitude': fac['lng'],
        'inspection_date': cand['date'],
        'original_score': original,
        'risk_score': risk,
        'priority_violations': pv,
        'priority_foundation_violations': pfv,
        'core_violations': cv,
        'total_violations': len(violations),
        'violations': [list(v) for v in violations],
        'inspection_type': cand['type'],
        'results': results,
        'source': cfg['source'],
        'source_url': f"{cfg['site']}#/{cfg['program']}/disclosure/facility/{fac['id']}/inspection/{cand['id']}",
        'source_id': f"{cfg['prefix']}_{cand['key']}",
        'metro': cfg['metro'],
    }


def _hh_candidates(cfg, fac, inspections, since, skipped):
    """A facility's inspection list -> the in-window inspections to read."""
    if not isinstance(inspections, list):
        raise _HhError(f'inspection list has an unexpected shape: {str(inspections)[:80]}')
    out, newest = [], ''
    for insp in inspections:
        if not isinstance(insp, dict):
            continue
        raw = str(insp.get('inspectionDate') or '')[:10]
        newest = max(newest, raw)
        date = sane_inspection_date(raw)
        iid = str(insp.get('id') or '').strip()
        if not date or not iid:
            skipped['inspection without a date or id'] += 1
            continue
        if date < since:
            continue
        rating = ' '.join(str(insp.get('hazardRating') or '').split())
        if cfg['results'] == 'rating' and rating not in PIMA_SHIP_RATINGS:
            skipped[f"rating '{rating or 'none'}'"] += 1
            continue
        try:
            score = int(round(float(insp.get('hazardScore'))))
        except (TypeError, ValueError):
            score = None
        itype = ' '.join(str(insp.get('inspectionType') or '').split())
        number = ' '.join(str(insp.get('inspectionNumber') or '').split())
        # The row's own word on whether anything was cited (Pima's flag,
        # Lubbock's count of every citation): the cross-check for a checklist
        # that comes back with no cited item.
        flag = insp.get('hasInfractions')
        try:
            count = int(insp.get('criticalInfractionCount'))
        except (TypeError, ValueError):
            count = None
        out.append({
            'fac': fac, 'id': iid, 'date': date, 'score': score, 'rating': rating,
            'type': itype or ' '.join(str(insp.get('inspectionReason') or '').split()),
            'key': number if cfg['prefix'] == 'lbk' and number else iid,
            'infractions': flag if isinstance(flag, bool) else None, 'count': count,
        })
    return out, newest


def _hh_chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _fetch_hedgehog(slug, since_date=None, limit=None):
    cfg = HH_PORTALS[slug]
    group, source = cfg['group'], cfg['source']
    # `asked` is the window the caller wants: holds are keyed to it, so the
    # next run starts there and widens again. `since` is what is read — the
    # portal's publication-lag margin earlier (never in test mode / the
    # history floor, where there is nothing to miss).
    asked = str(since_date)[:10] if since_date else cfg['history_start']
    margin = cfg.get('lag_margin_days', 0) if since_date else 0
    since = asked
    if margin:
        since = (datetime.strptime(asked, '%Y-%m-%d') - timedelta(days=margin)).strftime('%Y-%m-%d')
    started = time.time()
    with _HH_REQUESTS_LOCK:
        _HH_REQUESTS[slug] = 0
        _HH_STREAK[slug] = 0
        _HH_BLOCKED.pop(slug, None)
    log.info(f"Fetching {group} data (since={since}"
             + (f", {margin} days before the {asked} asked for" if margin else '') + f", limit={limit})")

    # Phase 0 — the facility list (atomic: any failure raises and ships nothing).
    rows = _hh_facility_rows(slug, since)
    skipped = defaultdict(int)
    facilities, newest, seen = [], '', set()
    for row in rows:
        if isinstance(row, dict):        # the source's own newest row, in scope or not
            newest = max(newest, str(row.get('lastInspectionDate') or '')[:10])
        fac = _hh_facility(cfg, row, skipped)
        if fac is None:
            continue
        if fac['id'] in seen:
            continue                     # a page-boundary shift repeated the row
        seen.add(fac['id'])
        if cfg['dated_list']:
            if not fac['last']:
                skipped['never inspected'] += 1
                continue
            if fac['last'] < since:
                continue
        facilities.append(fac)
    # Oldest-inspected facilities first, so a deadline cut drops the newest
    # tail (Mesquite shows no dates: its list order, by id, is used as is).
    facilities.sort(key=lambda f: (f['last'], f['id']))
    log.info(f"  {group}: {len(rows)} facilities listed, {len(facilities)} "
             + (f"inspected since {since}" if cfg['dated_list'] else 'to read (the list shows no dates)')
             + (f"; skipped {dict(skipped)}" if skipped else ''))

    # Phase 1 — each selected facility's inspection list, HH_WORKERS at a
    # time in order, stopping between chunks for the limit or the deadline.
    def list_one(fac):
        if _past_deadline():
            return 'deadline'
        try:
            data = _hh_json(slug, f"disclosure/program/{cfg['program']}/facility/{fac['id']}/inspectionDetails/")
        except _HhError as e:
            return e
        time.sleep(HH_DELAY)
        return data

    candidates, dropped, cut, bounded = [], defaultdict(int), False, False
    read, unread_lists = set(), []
    with ThreadPoolExecutor(max_workers=HH_WORKERS) as pool:
        for chunk in _hh_chunks(facilities, HH_WORKERS):
            if limit and len(candidates) >= limit:
                bounded = True
                break
            if _past_deadline() or _hh_blocked(slug):
                break
            for fac, got in zip(chunk, pool.map(list_one, chunk)):
                if got == 'deadline' or (isinstance(got, _HhError) and got.blocked):
                    continue                     # not tried: held with the unread tail below
                read.add(fac['id'])
                if isinstance(got, _HhError):
                    if got.permanent:
                        dropped['facility gone'] += 1       # a 400/404/410: nothing to re-read
                    else:
                        dropped['facility list unread'] += 1
                        unread_lists.append(fac['id'])
                    continue
                try:
                    cands, nw = _hh_candidates(cfg, fac, got, since, skipped)
                except _HhError:
                    dropped['facility list unread'] += 1
                    unread_lists.append(fac['id'])
                    continue
                newest = max(newest, nw)
                candidates.extend(cands)
    blocked = bool(_hh_blocked(slug))
    # An unread list leaves the facility's in-window inspections unknown, so
    # the whole window is held: counted (given up after 3 runs) unless the
    # portal was blocking, which is not that facility's fault.
    for fid in unread_lists:
        _hold_window_start(source, asked, counted=not blocked, key=fid)
    unread = len(facilities) - len(read)
    if unread and not bounded:
        # The deadline or the breaker cut the facility phase: the window is
        # held (not counted) and the group reports a partial read.
        cut = cut or not blocked
        _hold_window_start(source, asked, counted=False, key='facilities')
        dropped[f"facility list not read ({'blocked' if blocked else 'deadline'})"] += unread
    candidates.sort(key=lambda c: (c['date'], c['id']))
    if limit and len(candidates) > limit:
        bounded = True
        candidates = candidates[:limit]
    log.info(f"  {group}: {len(candidates)} inspections to read from {len(read)} "
             f"facility lists ({time.time() - started:.0f}s so far)")

    # Phase 2 — each inspection's checklist, oldest first. An unread one is
    # dropped and held (never shipped as clean).
    def read_one(cand):
        if _past_deadline():
            return 'deadline'
        try:
            data = _hh_json(slug, f"disclosure/program/inspectionResults/{cand['id']}")
        except _HhError as e:
            return e
        time.sleep(HH_DELAY)
        return data

    # `unread_checklists` could not be fetched or parsed (held, counted);
    # `unpublished` came back without a cited item although the inspection
    # row says there were infractions (held and counted too: the county may
    # still fill the form in, and after three runs it is given up).
    results, read_cands, unread_checklists, unpublished = [], set(), [], []
    with ThreadPoolExecutor(max_workers=HH_WORKERS) as pool:
        for chunk in _hh_chunks(candidates, HH_WORKERS):
            if _past_deadline() or _hh_blocked(slug):
                break
            for cand, got in zip(chunk, pool.map(read_one, chunk)):
                if got == 'deadline' or (isinstance(got, _HhError) and got.blocked):
                    continue
                read_cands.add(cand['id'])
                if isinstance(got, _HhError):
                    if got.permanent:
                        dropped['checklist gone'] += 1
                    else:
                        dropped['checklist unread'] += 1
                        unread_checklists.append(cand)
                    continue
                try:
                    violations = _hh_violations(got)
                except ValueError as e:
                    log.debug(f"  {group}: inspection {cand['id']} ({cand['date']}): {e}")
                    dropped['checklist unread'] += 1
                    unread_checklists.append(cand)
                    continue
                if not violations and not _hh_clean_is_believable(cfg, cand, got):
                    log.debug(f"  {group}: inspection {cand['id']} ({cand['date']}): no cited item on a "
                              f"checklist the inspection row says has infractions — not shipped as clean")
                    dropped['checklist unpublished'] += 1
                    unpublished.append(cand)
                    continue
                rec = _hh_record(cfg, cand['fac'], cand, violations)
                if rec is None:
                    skipped['unscored visit'] += 1
                    continue
                results.append(rec)
    blocked = bool(_hh_blocked(slug))
    for cand in unread_checklists + unpublished:
        _hold_freshness(source, cand['date'], counted=not blocked, key=cand['id'])
    pending = [c for c in candidates if c['id'] not in read_cands]
    if pending:
        cut = cut or not blocked
        for cand in pending:
            _hold_freshness(source, cand['date'], counted=False, key=cand['id'])
        dropped[f"checklist not read ({'blocked' if blocked else 'deadline'})"] += len(pending)

    # The group's status, worst first: a tripped breaker, too many unread
    # records, a deadline cut, a window the portal has not published yet.
    failed = len(unread_lists) + len(unread_checklists)
    tried = len(read) + len(read_cands)
    not_read = sum(n for why, n in dropped.items() if 'not read' in why)
    if blocked:
        _set_group_status(group, 'blocked', f"{_hh_blocked(slug)}; {not_read} facility lists/checklists not "
                                            f"read and {failed + len(unpublished)} unread, all held for the "
                                            f"next run")
    elif failed >= HH_MASS_FAILURE_MIN and failed > HH_MASS_FAILURE_FRACTION * tried:
        _set_group_status(group, 'error', f"{failed} of {tried} facility lists/checklists could not be read "
                                          f"(held for the next run)")
    elif (len(unpublished) >= HH_MASS_FAILURE_MIN
            and len(unpublished) > HH_MASS_UNPUBLISHED_FRACTION * len(read_cands)):
        _set_group_status(group, 'error', f"{len(unpublished)} of {len(read_cands)} checklists show no cited "
                                          f"item although their inspection rows say there were infractions "
                                          f"(held); a portal change?")
    elif cut:
        _set_group_status(group, 'skipped', f"fetch deadline reached; {not_read} facility lists/checklists "
                                            f"not read (held for the next run)")
    elif newest and since > newest and not bounded:
        _set_group_status(group, 'error', f"the window starts {since} but the portal's newest inspection is "
                                          f"{newest}: nothing to read. Seed the group once with --since-date "
                                          f"and --freshness-file so a latest is recorded")
    if dropped:
        log.warning(f"  {group}: dropped " + ', '.join(f'{n} ({why})' for why, n in sorted(dropped.items()))
                    + " — none shipped as clean")
    # The source's own newest row: a dated list shows it whatever was read,
    # an undated one (Mesquite) only once every facility's list was read.
    if not bounded and (cfg['dated_list'] or not (cut or blocked)):
        _note_source_lag(group, newest)

    # Newest day first; within a day the worst visit first (D1 keeps one
    # inspection per restaurant per day, _same_day_rank picks the worst).
    results.sort(key=lambda r: (r['risk_score'], -r['priority_violations'], -r['total_violations'],
                                r['source_id']))
    results.sort(key=lambda r: r['inspection_date'], reverse=True)
    with _HH_REQUESTS_LOCK:
        n_req = _HH_REQUESTS[slug]
    log.info(f"{group}: {len(results)} inspections from {len(read)} facilities "
             f"in {time.time() - started:.0f}s ({n_req} requests; source newest {newest or 'n/a'}"
             + (f"; skipped {dict(skipped)}" if skipped else '') + ")")
    return results


def fetch_pima(since_date=None, limit=None):
    """Pima County, AZ (Tucson, Oro Valley, Marana, Sahuarita, Green Valley,
    ...): the county's Hedgehog portal. No numeric score is published, so
    original_score is None and risk_score is 100 - 5P - 2PF - C over the
    citations' agency-stated severities, capped below 70 for a 'Fail', 'IHH'
    or 'Probation' rating; the rating itself goes to results. Metro chip
    'Tucson'. See the section comment for the portal's shape."""
    return _fetch_hedgehog('pima', since_date, limit)


def fetch_lubbock(since_date=None, limit=None):
    """City of Lubbock, TX Environmental Health: official 0-100 hazardScore
    (both original_score and risk_score), the county's letter in results
    ('Grade A'), agency-stated severities per citation. Temporary-event
    booths (TF15), schools and mobile units are dropped."""
    return _fetch_hedgehog('lubbock', since_date, limit)


def fetch_mesquite(since_date=None, limit=None):
    """City of Mesquite, TX (DFW) Environmental Health: official 0-100
    hazardScore, agency-stated severities, no rating and no observation text
    (violation lines are the cited item's title plus the rule's short name).
    The list hides facility types and dates, so every facility's inspection
    list is read each run and missing coordinates are geocoded by main()."""
    return _fetch_hedgehog('mesquite', since_date, limit)


# ─── WILLIAMSON COUNTY, TX (WCCHD inspection-scores zip snapshot) ───────────
# The Williamson County and Cities Health District (Round Rock, Georgetown,
# Cedar Park, Leander, Hutto, Taylor, Liberty Hill, Jarrell and the
# Williamson-county parts of Austin and Pflugerville) publishes its scores
# as a Yelp-LIVES-style zip on Google Cloud Storage: businesses.csv
# (business_id,name,address,city,state), inspections.csv (business_id,date
# YYYYMMDD,score,result,type) and feed_info.xlsx (feed_date). The one
# ~260 KB object IS the whole history (2021-12 on; 12,145 permits and 6,734
# inspections in the 2026-10-29 snapshot), so a run is a single GET and the
# weekly window is a client-side filter on the date column. A snapshot that
# arrives truncated or reshaped (a missing member, no feed_date, too few
# rows, more than WM_MAX_BAD_ROW_SHARE unparseable rows) is REFUSED with a
# RuntimeError rather than shipped as "fewer inspections": the merge keeps
# last week's records and the run goes red. The feed carries no violation
# text, no coordinates and no zip: records ship with violations [] and
# main() geocodes them (Census batch, as for Austin).
#
# Scoring: `score` is the county's official 100-point Inspection Score
# (100 - demerits on its form; < 70 is a Fail, which holds for every Fail
# row in the feed). It is copied to original_score and risk_score
# (clamped to 0..100); a score <= 0 is an arithmetic artefact (eight rows
# from 2022-23 run to -532) and is dropped with a warning, not shipped as
# unscored. 'IHH' (Imminent Health Hazard: a closure "not score-related",
# two thirds of them with a passing score) caps risk_score at
# WM_IHH_RISK_CAP so the grade is F, the way Orange County's and San
# Diego's closures are capped; original_score keeps the published number.
#
# Scope: the permit number encodes the program, so temporary event booths
# (-TFP), mobile units (-MF: a truck's row carries its commissary or base
# address), on-site sewage permits (-SAN) and school kitchens (-ISD,
# -SFEC, -FFES..., and the <city>S- prefixes the ISDs' contract kitchens
# use: RRS-, GES-, LES-, HUS-, TAS-...) are dropped, as the Tennessee and
# Omaha fetchers drop theirs; -FFEC (a commissary/shared kitchen) is food
# and stays. The district's own permits are dropped by address (every one
# of the 35 permits at its 355 Texas Ave office is internal: 'TEST',
# 'TEST 355', 'ABC Food', 'WCCHD', 'Food Sampling', a SNAP-Ed station) and
# by 'TEST'-style name elsewhere. Rows whose business_id has no
# businesses.csv row (13 food permits, 0.44% of the food rows in the
# 2026-10-29 file), no name or no usable city are dropped with a WARNING
# count.
#
# Late rows and the re-read margin: businesses.csv lists ISSUED permits, so
# a new restaurant's pre-opening inspections sit in inspections.csv for
# months before their business row appears (every 2026-numbered orphan in
# the 2026-10-29 file carried a permit number above the newest one in
# businesses.csv; the oldest was 85 days past its inspection), and the
# feed's 2026-H2 volume suggests rows may be entered late too. A weekly
# window that starts the day before the newest shipped row would never see
# them again, so in weekly mode the window never starts later than
# WM_REREAD_DAYS ago: the whole file is one GET anyway, and the data.js
# merge and the D1 upserts are idempotent, so re-shipping a few hundred
# rows a week costs nothing. A freshness hold is deliberately NOT used for
# an orphan: it would pin the recorded latest at the orphan's date for
# three runs (the report would flag the group STALE for a lag that is the
# source's) and then give the record up, sooner than the orphans observed
# resolve. Vocabulary drift fails closed: a result outside Pass/Fail/IHH is
# an unparseable row (the result is what carries a closure, so it never
# ships on its score alone) and more than WM_MAX_BAD_ROW_SHARE of them
# refuses the snapshot, as does a businesses.csv that covers less than
# 1 - WM_MAX_ORPHAN_SHARE of the food-establishment rows.
#
# Identity: 'ROUND ROCK' / 'Round rock' / 'round rock' are one city, so
# name, address and city are normalised (whitespace collapsed, a stray
# trailing comma or glued zip code removed, ALL CAPS title-cased) before
# make_restaurant_id. Suite/unit tails stay exactly as published. A same-
# day exact duplicate row is the same inspection listed twice and is
# emitted once; distinct same-day rows (a failed routine and a passed
# follow-up) are each emitted, worst first, the later ones with a '_2',
# '_3' suffix on source_id so the ids stay distinct (D1 keeps the worst of
# the day either way). Output is newest-first, so `limit` keeps the newest
# records (test mode).
import csv
import io
import zipfile
import xml.etree.ElementTree as ET

WM_ZIP_URL = 'https://storage.googleapis.com/ehyelp/WCCHD%20Food%20Inspections.zip'
WM_SOURCE_PAGE = 'https://www.wcchd.org/healthy_spaces/inspection_scores.php'
WM_SOURCE = 'Williamson County and Cities Health District'
WM_GROUP = 'Williamson County'
WM_MEMBERS = ('feed_info.xlsx', 'businesses.csv', 'inspections.csv')
WM_BUSINESS_FIELDS = ('business_id', 'name', 'address', 'city', 'state')
WM_INSPECTION_FIELDS = ('business_id', 'date', 'score', 'result', 'type')
# Request pacing (module globals so tests can patch them).
WM_TIMEOUT = 120              # seconds for the one GET (a ~260 KB object)
WM_RETRIES = 3                # attempts: connection errors, timeouts, 429 and 5xx retry
WM_RETRY_DELAY = 10           # seconds before the second attempt, doubled each time
# Snapshot sanity. The 2026-10-29 feed held 6,734 inspections and 12,145
# permits (a registry that only grows: temporary permits stay listed) with
# 0 unparseable rows and 24 food-establishment rows (0.44%) without a
# businesses.csv row; a file far outside that is a truncated upload or a
# changed layout, not a quiet month.
WM_MIN_INSPECTION_ROWS = 1000
WM_MIN_BUSINESS_ROWS = 6000
WM_MAX_BAD_ROW_SHARE = 0.01   # blank id, bad date or score, or a result outside WM_RESULTS
WM_MAX_ORPHAN_SHARE = 0.05    # food rows with no businesses.csv row (0.4% is normal)
# Weekly mode re-reads at least this far back (see the section comment): a
# pre-opening inspection ships once its business row appears, a row entered
# late ships on the next run. 0 disables the margin.
WM_REREAD_DAYS = 180
WM_IHH_RISK_CAP = 55          # a closure grades F (the Orange County / San Diego rule)
# Keyed by the upper-cased feed value; anything else is an unparseable row.
WM_RESULTS = {'PASS': 'Pass', 'FAIL': 'Fail', 'IHH': 'Imminent Health Hazard - closed'}
WM_TYPES = {'Routine': 'Routine', 'Followup': 'Follow-up'}
# Programs that are not fixed food establishments, by permit-number code.
WM_OUT_OF_SCOPE = (
    ('temporary event booth', re.compile(r'-TFP$')),
    ('mobile unit', re.compile(r'-MF$')),
    ('sewage/sanitation permit', re.compile(r'-SAN$')),
    ('school kitchen', re.compile(r'-ISD$|-SFEC$|-FFES(?:-|$)|^[A-Z]{2}S-\d')),
)
# The district's own permits: every permit at its 355 Texas Ave office
# (matched on the normalised address, with or without 'Ave'/'Avenue') and
# the 'TEST' / 'Test Project Name' / 'TEST Preopen Assignment' names it
# uses elsewhere; a real "Test Kitchen" stays.
WM_TEST_NAME_RE = re.compile(r'^\s*test(?:\s*$|\s+(?:project|preopen|permit|record|account)\b)', re.I)
WM_TEST_ADDRESS_RE = re.compile(r'^355\s+Texas\b', re.I)
_WM_XLSX_NS = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
# Tokens .title() must not lower-case: Texas road codes and compass points
# in addresses ('FM 1105', 'IH 35', 'NE Inner Loop'), company codes in names.
_WM_ROAD_CODES = re.compile(r'\b(Fm|Rm|Rr|Cr|Sh|Ih|Us|Tx|Ne|Nw|Se|Sw)\b')
_WM_NAME_CODES = re.compile(r'\b(Llc|Inc|Dba|Bbq|Atx|Heb|Tx|Usa|Ii|Iii|Iv)\b')


class _WmDeadlineCut(RuntimeError):
    """The fetch deadline stopped the download's retries (reported 'skipped')."""


def _wm_title(s, road_codes=False):
    """.title() for an ALL-CAPS source string; mixed case is kept as
    published. Possessives stay possessive ("Joe's", "Joe’s"), ordinals
    stay ordinal ("8th St") and the code tokens stay upper."""
    s = ' '.join((s or '').split())
    if not s.isupper():
        return s
    t = s.title()
    t = re.sub(r"(['’])S\b", r"\1s", t)
    t = re.sub(r'\b(\d+)(St|Nd|Rd|Th)\b', lambda m: m.group(1) + m.group(2).lower(), t)
    codes = _WM_ROAD_CODES if road_codes else _WM_NAME_CODES
    return codes.sub(lambda m: m.group(1).upper(), t)


def _wm_city(raw):
    """'ROUND ROCK' / 'Round rock' / 'round rock' -> 'Round Rock'. A stray
    trailing comma ('TAYLOR,'), quote or glued zip ('Leander 78641') goes;
    '' when nothing usable is left (the caller then drops the row)."""
    c = ' '.join((raw or '').split())
    c = re.sub(r'\s*\d{5}(?:-\d{4})?$', '', c)
    c = ' '.join(c.strip(' ,.;:`"\'').split())
    if not c or c[0].isdigit():
        return ''
    return c.title()


def _wm_address(raw):
    """Whitespace collapsed, a trailing comma dropped ('445 E Morrow St,'),
    ALL CAPS title-cased with the road codes kept; suites stay as published."""
    a = ' '.join((raw or '').split()).strip(' ,;')
    a = re.sub(r'\s+,', ',', a)
    return _wm_title(a, road_codes=True)


def _wm_out_of_scope(business_id):
    """The scope label a permit number falls under, or None for a fixed
    food establishment."""
    for label, pattern in WM_OUT_OF_SCOPE:
        if pattern.search(business_id):
            return label
    return None


def _wm_read_feed_info(xlsx_bytes):
    """feed_info.xlsx (one sheet: a header row and a value row) -> dict of
    column -> value, read with zipfile + xml.etree. feed_date comes back as
    'YYYY-MM-DD'. Raises ValueError when there is no sheet, no value row or
    no parseable feed_date."""
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as x:
        names = x.namelist()
        sheets = sorted(n for n in names if re.fullmatch(r'xl/worksheets/sheet\d+\.xml', n))
        if not sheets:
            raise ValueError('feed_info.xlsx has no worksheet')
        shared = []
        if 'xl/sharedStrings.xml' in names:
            for si in ET.fromstring(x.read('xl/sharedStrings.xml')).iter(f'{_WM_XLSX_NS}si'):
                shared.append(''.join(t.text or '' for t in si.iter(f'{_WM_XLSX_NS}t')))
        sheet = ET.fromstring(x.read(sheets[0]))
    cells = {}
    for c in sheet.iter(f'{_WM_XLSX_NS}c'):
        m = re.fullmatch(r'([A-Z]+)(\d+)', c.get('r') or '')
        if not m:
            continue
        kind = c.get('t')
        if kind == 'inlineStr':
            val = ''.join(t.text or '' for t in c.iter(f'{_WM_XLSX_NS}t'))
        else:
            v = c.find(f'{_WM_XLSX_NS}v')
            val = (v.text or '') if v is not None else ''
            if kind == 's':
                try:
                    val = shared[int(val)]
                except (ValueError, IndexError):
                    raise ValueError(f'feed_info.xlsx cell {c.get("r")} points at a missing shared string')
        cells.setdefault(int(m.group(2)), {})[m.group(1)] = val.strip()
    filled = sorted(r for r, cols in cells.items() if any(cols.values()))
    if len(filled) < 2:
        raise ValueError('feed_info.xlsx has no value row under its header')
    header, values = cells[filled[0]], cells[filled[1]]
    info = {key.lower(): values.get(col, '') for col, key in header.items() if key}
    m = re.fullmatch(r'(\d{4})(\d{2})(\d{2})(?:\.0+)?', info.get('feed_date', ''))
    if not m:
        raise ValueError(f"feed_info.xlsx has no parseable feed_date ({info.get('feed_date')!r})")
    iso = f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
    datetime.strptime(iso, '%Y-%m-%d')          # ValueError when not a date
    info['feed_date'] = iso
    return info


def _wm_download(url=None):
    """GET the snapshot. Connection errors, timeouts, 429 and 5xx are retried
    WM_RETRIES times with a growing delay, never past the fetch deadline
    (_WmDeadlineCut); any other status raises at once: the district's page
    has linked the object under a wrong name before ('.zip1' -> 403), so a
    403/404 means the feed moved or went private, not that it is empty.
    Returns (bytes, response headers)."""
    url = url or WM_ZIP_URL
    delay, last = WM_RETRY_DELAY, ''
    for attempt in range(1, WM_RETRIES + 1):
        try:
            r = requests.get(url, headers={'User-Agent': CHROME_UA, 'Accept': 'application/zip,*/*'},
                             timeout=WM_TIMEOUT)
        except requests.RequestException as e:
            last = f'{type(e).__name__}: {e}'
        else:
            if r.status_code == 200:
                return r.content, r.headers
            if r.status_code != 429 and r.status_code < 500:
                raise RuntimeError(f'Williamson County: HTTP {r.status_code} for {url} — the snapshot '
                                   f'object has moved or is no longer public')
            last = f'HTTP {r.status_code}'
        if attempt < WM_RETRIES:
            if _past_deadline():
                raise _WmDeadlineCut(f'fetch deadline reached while retrying the snapshot download ({last})')
            log.warning(f"Williamson County: snapshot download failed ({last}); retry {attempt} in {delay}s")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f'Williamson County: could not download the snapshot after {WM_RETRIES} attempts ({last})')


def _wm_unzip(content):
    """The three LIVES members by basename (a folder inside the zip would be
    tolerated); raises RuntimeError on a non-zip or a missing member."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            by_base = {}
            for name in sorted(z.namelist()):
                by_base.setdefault(os.path.basename(name), name)
            missing = [m for m in WM_MEMBERS if m not in by_base]
            if missing:
                raise RuntimeError(f'Williamson County: snapshot zip lacks {missing} (members: {sorted(by_base.values())})')
            return {m: z.read(by_base[m]) for m in WM_MEMBERS}
    except zipfile.BadZipFile as e:
        raise RuntimeError(f'Williamson County: the download is not a zip ({e}; {len(content)} bytes)')


def _wm_rows(data, fields, member):
    """csv.DictReader over a CSV member (UTF-8 with or without a BOM; a
    cp1252 re-export is accepted with a warning, since its curly quotes
    decode to the same characters) -> list of dicts with lower-cased keys
    and '' for missing values. Raises RuntimeError when the bytes decode as
    neither (a lossy decode would re-key every restaurant with a non-ASCII
    name and strand its history) or the header lacks a LIVES column."""
    try:
        text = data.decode('utf-8-sig')
    except UnicodeDecodeError:
        try:
            text = data.decode('cp1252')
        except UnicodeDecodeError as e:
            raise RuntimeError(f'Williamson County: {member} is neither UTF-8 nor cp1252 ({e}); '
                               f'refusing to re-key restaurants on a garbled decode')
        log.warning(f"Williamson County: {member} is not UTF-8; decoded as cp1252")
    reader = csv.DictReader(io.StringIO(text))
    header = [(h or '').strip().lower() for h in (reader.fieldnames or [])]
    missing = [f for f in fields if f not in header]
    if missing:
        raise RuntimeError(f'Williamson County: {member} lacks column(s) {missing} (header: {reader.fieldnames})')
    rows = []
    for row in reader:
        rows.append({(k or '').strip().lower(): (v or '').strip() for k, v in row.items()
                     if k is not None and not isinstance(v, list)})
    return rows


def fetch_williamson(since_date=None, limit=None):
    since = requested = str(since_date)[:10] if since_date else None
    if since and WM_REREAD_DAYS:
        # Never start later than the re-read margin: a row the district
        # adds late (a pre-opening inspection whose business row appears
        # months after it) must still fall inside a later run's window.
        floor = (datetime.now() - timedelta(days=WM_REREAD_DAYS)).strftime('%Y-%m-%d')
        if since > floor:
            since = floor
    log.info(f"Fetching Williamson County (WCCHD) snapshot (since={since}"
             + (f", widened from {requested} by the {WM_REREAD_DAYS}-day re-read margin"
                if since != requested else '') + f", limit={limit})")
    if _past_deadline():
        _set_group_status(WM_GROUP, 'skipped', 'fetch deadline reached before the snapshot was read')
        return []
    try:
        content, headers = _wm_download()
    except _WmDeadlineCut as e:
        log.warning(f"Williamson County: {e}")
        _set_group_status(WM_GROUP, 'skipped', str(e))
        return []
    members = _wm_unzip(content)
    try:
        info = _wm_read_feed_info(members['feed_info.xlsx'])
    except (ValueError, ET.ParseError, zipfile.BadZipFile) as e:
        raise RuntimeError(f'Williamson County: unreadable feed_info.xlsx ({e})')
    businesses = _wm_rows(members['businesses.csv'], WM_BUSINESS_FIELDS, 'businesses.csv')
    inspections = _wm_rows(members['inspections.csv'], WM_INSPECTION_FIELDS, 'inspections.csv')
    if len(inspections) < WM_MIN_INSPECTION_ROWS or len(businesses) < WM_MIN_BUSINESS_ROWS:
        raise RuntimeError(f'Williamson County: snapshot has only {len(inspections)} inspection rows and '
                           f'{len(businesses)} business rows (expect >= {WM_MIN_INSPECTION_ROWS} / '
                           f'{WM_MIN_BUSINESS_ROWS}); refusing a truncated feed')
    by_id = {}
    for b in businesses:
        if b['business_id']:
            by_id.setdefault(b['business_id'], b)

    parsed, bad, unknown_results = [], 0, defaultdict(int)
    for row in inspections:
        bid = row['business_id']
        m = re.fullmatch(r'(\d{4})(\d{2})(\d{2})', row['date'])
        day = sane_inspection_date(f'{m.group(1)}-{m.group(2)}-{m.group(3)}') if m else None
        try:
            score = int(float(row['score']))
        except (ValueError, OverflowError):
            score = None
        result = row['result'].upper()
        if result not in WM_RESULTS:
            unknown_results[row['result']] += 1
        if not bid or not day or score is None or result not in WM_RESULTS:
            bad += 1
            continue
        parsed.append((bid, day, score, result, row['type']))
    if bad > WM_MAX_BAD_ROW_SHARE * len(inspections):
        raise RuntimeError(f'Williamson County: {bad} of {len(inspections)} inspection rows unparseable '
                           f'(> {WM_MAX_BAD_ROW_SHARE:.0%}'
                           + (f'; results outside {sorted(WM_RESULTS)}: {dict(unknown_results)}'
                              if unknown_results else '')
                           + '); refusing a reshaped feed')
    if unknown_results:
        log.warning(f"Williamson County: dropped {sum(unknown_results.values())} rows whose result is "
                    f"outside {sorted(WM_RESULTS)} ({dict(unknown_results)}); a result the fetcher "
                    f"cannot read may be a closure, so it never ships on its score alone")
    # businesses.csv must cover the food-establishment rows: a truncated or
    # re-keyed permit file would otherwise drop most of the window as
    # orphans under a WARNING while the group reported 'ok'.
    food_rows = [p for p in parsed if _wm_out_of_scope(p[0]) is None]
    orphans = sum(1 for p in food_rows if p[0] not in by_id)
    if food_rows and orphans > WM_MAX_ORPHAN_SHARE * len(food_rows):
        raise RuntimeError(f'Williamson County: {orphans} of {len(food_rows)} food-establishment rows have '
                           f'no businesses.csv row (> {WM_MAX_ORPHAN_SHARE:.0%}; 0.4% is normal); refusing '
                           f'a snapshot whose businesses.csv does not cover its inspections')
    newest = max(p[1] for p in parsed)
    window = [p for p in parsed if since is None or p[1] >= since]
    in_margin = sum(1 for p in window if requested and p[1] < requested)
    log.info(f"  Williamson County: feed_date {info['feed_date']}, newest inspection {newest}; "
             f"{len(inspections)} inspection rows ({bad} unparseable), {len(businesses)} permits "
             f"({orphans} food rows without one), {len(window)} rows since {since or 'the start of the file'}"
             + (f" ({in_margin} of them before {requested}, in the re-read margin)" if in_margin else '')
             + f"; {len(content)} bytes, etag {headers.get('ETag')}, last-modified {headers.get('Last-Modified')}")

    quality, scope = defaultdict(int), defaultdict(int)
    built, seen = [], set()
    for bid, day, score, result, kind in window:
        label = _wm_out_of_scope(bid)
        if label:
            scope[label] += 1
            continue
        biz = by_id.get(bid)
        if biz is None:
            quality['no businesses.csv row (a permit not issued yet)'] += 1
            continue
        name = _wm_title(biz['name'])
        if not name:
            quality['no name'] += 1
            continue
        address = _wm_address(biz['address'])
        if WM_TEST_NAME_RE.match(name) or WM_TEST_ADDRESS_RE.match(address):
            scope["the district's test permit"] += 1
            continue
        city = _wm_city(biz['city'])
        if not city:
            quality['no usable city'] += 1
            continue
        if score <= 0:
            quality['score <= 0 (unscored artefact)'] += 1
            continue
        dup_key = (bid, day, score, result, kind)
        if dup_key in seen:
            scope['exact duplicate row'] += 1
            continue
        seen.add(dup_key)
        risk = max(0, min(100, score))
        if result == 'IHH':
            risk = min(risk, WM_IHH_RISK_CAP)
        itype = WM_TYPES.get(kind, kind)
        state = (biz['state'] or '').strip().upper()
        rec = {
            'name': name,
            'address': address,
            'city': city,
            'state': state if re.fullmatch(r'[A-Z]{2}', state) else 'TX',
            'zip': '',
            'latitude': None,
            'longitude': None,
            'inspection_date': day,
            'original_score': score,
            'risk_score': risk,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': itype,
            'results': WM_RESULTS[result],
            'source': WM_SOURCE,
            'source_url': WM_SOURCE_PAGE,
            'source_id': f"wcchd_{bid}_{day.replace('-', '')}",
            'metro': '',
        }
        # Worst first within a business-day: a closure, then the lowest
        # score, then the routine visit before its follow-up.
        rank = (0 if result == 'IHH' else 1, risk, 0 if itype == 'Routine' else 1)
        built.append((bid, day, rank, rec))

    same_day = defaultdict(list)
    for item in built:
        same_day[(item[0], item[1])].append(item)
    ordered = []
    for (bid, day), items in same_day.items():
        items.sort(key=lambda it: it[2])
        for n, (_, _, rank, rec) in enumerate(items):
            if n:
                rec['source_id'] += f'_{n + 1}'
            ordered.append((-int(day.replace('-', '')), bid, rank, rec))
    ordered.sort(key=lambda it: it[:3])
    records = [it[3] for it in ordered]

    if quality:
        log.warning(f"Williamson County: dropped {sum(quality.values())} rows in the window — "
                    + ', '.join(f'{n} {why}' for why, n in sorted(quality.items())))
    if scope:
        log.info(f"  Williamson County: skipped {sum(scope.values())} rows — "
                 + ', '.join(f'{n} {why}' for why, n in sorted(scope.items())))
    truncated = bool(limit) and len(records) > limit
    if truncated:
        records = records[:limit]
    else:
        _note_source_lag(WM_GROUP, newest)
    log.info(f"Williamson County: {len(records)} inspections"
             + (f' (limit {limit})' if truncated else ''))
    return records


# ─── DFW CITIES WITHOUT A PORTAL ON MHD: McKINNEY, LEWISVILLE, DENTON ────────
# Three DFW cities that run their own food programs outside the
# MyHealthDepartment portal (probed live 2026-11-02/03): McKinney publishes
# a city ArcGIS MapServer fed from its permitting system, Lewisville a
# one-request JSON web service behind its Vue app, Denton a CKAN DataStore
# refreshed daily. None has a per-record detail phase: each fetcher reads its
# window completely or RAISES (a raise records the group 'error', keeps its
# recorded latest where it was and the next lookback re-reads the window).
# The only hold any of them writes is McKinney's for an inspection whose
# item points do not add up to the city's total (see fetch_mckinney).
# A window in which most rows carry no usable score raises too
# (DFW_HOLE_UNSCORED_MIN): a renamed or re-typed score column must be a red
# run, not a green 'empty' one. Irving (the largest DFW hole) has no live
# machine-readable source: both of its ArcGIS items are abandoned snapshots
# (2020-02, 2024-06) and its scores page sits behind a Cloudflare JavaScript
# challenge, so it is not fetched.
#
# Scope, the same rule in all three cities: schools and child-care kitchens
# are not restaurants and are dropped by name (DFW_HOLE_NON_RESTAURANT_RE,
# the only signal Lewisville and Denton give; McKinney also has its school
# layer and permit classes), as are workplace vending 'micro markets'.
# Mobile units are dropped only where the source says so (McKinney's permit
# layers and classes): every Denton row carries a fixed address, so a
# 'Mobile' in a Denton name is left alone, and Lewisville's stands are its
# unscored 'PASS' rows, dropped as unscored.

DFW_HOLE_RETRIES = 3          # attempts per request on 429/5xx/timeouts
DFW_HOLE_BACKOFF = 3.0        # seconds before the 2nd attempt; grows per attempt
DFW_HOLE_TIMEOUT = 120
DFW_HOLE_PAGE_DELAY = 0.3     # pause between pages of one layer / table
DFW_HOLE_RETRY_STATUSES = (429, 500, 502, 503, 504)
# A window of at least this many rows in which more than half carry no
# usable score is a format change, not data: the fetch raises.
DFW_HOLE_UNSCORED_MIN = 20
# Conservative on purpose: 'academy' alone is a sporting-goods store and
# 'school' alone a culinary one, so only the unambiguous forms are listed
# ('LISD - ...', 'Ogle Elementary', 'Kiddie Academy', 'Applebee Montessori
# Academy', 'Aramark Micromarket').
DFW_HOLE_NON_RESTAURANT_RE = re.compile(
    r"\b(?:elementary|middle school|high school|[a-z]*isd|day ?care|child ?care|pre-?school|"
    r"learning center|early childhood|early education|montessori|kiddie academy|goddard school|"
    r"primrose school|preparatory school|children'?s academy|christian academy|classical academy|"
    r"micro ?markets?)\b", re.I)


def _dfw_hole_get(url, params=None, label='', verify=True, timeout=None):
    """GET a JSON endpoint with the expansion's retry ladder. 429/5xx answers,
    timeouts and connection resets are retried DFW_HOLE_RETRIES times with
    a growing pause (never past the fetch deadline); any other status, a
    non-JSON body (Cloudflare's HTML block page, a WAF challenge) or an
    exhausted ladder RAISES RuntimeError so the caller ships nothing rather
    than a partial window. A TLS verification failure raises at once:
    retrying it cannot help and verification is never relaxed."""
    last = ''
    attempt = 0
    for attempt in range(1, DFW_HOLE_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers={'User-Agent': CHROME_UA},
                             timeout=timeout or DFW_HOLE_TIMEOUT, verify=verify)
        except requests.exceptions.SSLError as e:
            raise RuntimeError(f'{label}: TLS verification failed for {url}: {e}')
        except requests.RequestException as e:
            last = f'{type(e).__name__}: {e}'
            r = None
        if r is not None:
            ctype = (r.headers.get('content-type') or '').lower()
            if r.status_code in DFW_HOLE_RETRY_STATUSES:
                last = f'HTTP {r.status_code}'
            elif r.status_code != 200:
                blocked = ' (an HTML answer: blocked by the host?)' if 'html' in ctype else ''
                raise RuntimeError(f'{label}: HTTP {r.status_code} from {url}{blocked}')
            else:
                try:
                    return r.json()
                except ValueError:
                    raise RuntimeError(f'{label}: non-JSON answer from {url} '
                                       f'({ctype or "no content-type"}, {len(r.content)} bytes)')
        if attempt >= DFW_HOLE_RETRIES or _past_deadline():
            break
        time.sleep(DFW_HOLE_BACKOFF * attempt)
    raise RuntimeError(f'{label}: {last} after {attempt} attempt(s) ({url})')


def _dfw_hole_title(s):
    """.title() for an ALL-CAPS source string (possessives kept: "Joe's");
    mixed-case strings are only whitespace-collapsed. Deterministic, so
    make_restaurant_id is stable run to run."""
    s = ' '.join((s or '').split())
    if s.isupper():
        s = re.sub(r"'S\b", "'s", s.title())
    return s


def _dfw_hole_coord(v):
    """A float coordinate or None: '', None, 0 and null-island are 'not geocoded'."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f and abs(f) <= 180 else None


_DFW_HOLE_PHONE_RE = re.compile(r'\(?\b\d{3}\)?[-. ]\d{3}[-. ]\d{4}\b')
_DFW_HOLE_EMAIL_RE = re.compile(r'\S+@\S+\.\w+')


def _dfw_hole_scrub(text):
    """One line of violation text: whitespace collapsed, phone numbers and
    e-mail addresses removed (an inspector's note may quote a contact)."""
    t = ' '.join((text or '').split())
    t = _DFW_HOLE_PHONE_RE.sub('[phone]', t)
    t = _DFW_HOLE_EMAIL_RE.sub('[email]', t)
    return t.strip()


# ── McKinney ────────────────────────────────────────────────────────────────
# City-hosted ArcGIS Server (maps.mckinneytexas.org) fed from the permitting
# system: layer 4 'Food Inspections' is one row per inspection with the 47
# Texas Food Establishment Inspection Report items as integer point columns,
# the inspector's free-text report in custom_comments, point geometry,
# HEALTH####-##### permit numbers (link_number) and FOOD-######-YYYY
# inspection numbers. 17,035 inspections 2017-10 .. today, ~30 a week,
# server-side date filter verified (actual_end_date is an epoch-ms field
# holding LOCAL wall-clock time flagged as UTC, so its UTC date is the
# inspection's local date). total_score is DEMERIT points (lower is better),
# never a 0-100 score: the 47 item columns hold exactly 0 or the item's TX
# form weight (verified on 503 live inspections: items 1-20 = 3 points,
# 21-34 = 2, 35-47 = 1, and the points add up to total_score on every one),
# so each cited item's tier comes from its points like Richardson's
# HealthTrak (3 = priority, 2 = priority foundation, 1 = core) and
# risk_score is the TX form's own score, 100 - demerits, as Richardson,
# Dallas and Plano ship it; original_score keeps the city's demerit total.
# The points-add-up invariant is checked per inspection: a row where they do
# not is dropped and held, and a window where many do not raises, so a
# change to the item columns can never read as a page of clean 100s.
# 34-39% of inspection rows carry no geometry; layer 0 'Food Permits'
# (738 rows with coordinates) fills most of those by link_number and the
# rest are geocoded downstream.
MCK_BASE = ('https://maps.mckinneytexas.org/mckinney/rest/services/MapServices/'
            'FoodPermitsAndInspections/MapServer')
MCK_INSPECTIONS_LAYER = f'{MCK_BASE}/4'
MCK_PERMITS_LAYER = f'{MCK_BASE}/0'
# Permits tracked on these layers are not fixed-address food establishments:
# 1 'Mobile & Temporary Food Vendors', 2 'Trades Day & Farmers Market',
# 3 'Schools & Daycares' (McKinney ISD cafeterias). Their inspections are
# dropped (see _mck_non_restaurant_permits).
MCK_NON_RESTAURANT_LAYERS = (1, 2, 3)
# Layer-0 work classes that are not fixed-address restaurants either: a
# mobile unit, a farmers-market stall, a splash pad, a workplace vending
# micro market (the other classes, 712 'Food Establishment', 'Restaurant',
# 'Concession Stand' and a 'Central Preparation Facility', are kept).
MCK_NON_RESTAURANT_CLASSES = {'Mobile Food Establishment', 'Farmers Market Temporary',
                              'Public Interactive Water Feature', 'Micro Market - Vending Machine'}
# School, child-care and micro-market permits that are on no permit layer at
# all (a quarter of the 90-day inspections link to no layer-0 permit) are
# told by name with the rule shared by the three cities.
MCK_SCHOOL_NAME_RE = DFW_HOLE_NON_RESTAURANT_RE
MCK_HISTORY_START = '2017-10-01'          # the layer starts 2017-10-16
MCK_TEST_WINDOW_DAYS = 60                 # a bounded read (limit set, no since) stays one page
MCK_PAGE = 2000                           # the server's maxRecordCount
MCK_SKIP_STATUSES = {'Void'}              # voided inspections (19 in the history)
MCK_SKIP_PURPOSES = {'3 - Pre-Opening'}   # permit walk-throughs of a place not yet open
# Inspections whose item points do not add up to total_score are dropped and
# held one by one; this many of them, making up more than this share of the
# window, mean the item columns changed and the whole fetch raises.
MCK_MISMATCH_MIN = 10
MCK_MISMATCH_MAX_SHARE = 0.05
MCK_SOURCE = 'McKinney Health Inspections'
MCK_APP_URL = ('https://mck.maps.arcgis.com/apps/instant/basic/index.html'
               '?appid=1f11251105aa491fbb23ace023b5b61f')
# The 47 TX form items in form order: (layer-4 column, public title). The
# title is prefixed to the inspector's observation so the classifier and the
# summaries see what the item is about, EXCEPT on items 20 (sewage) and 34
# (pests): a bare title naming an F-forcing hazard must never ride along
# with a door-gap or grease-trap note, so those two carry the inspector's
# observation alone. Items 1-20 weigh 3 points, 21-34 two, 35-47 one.
MCK_ITEMS = (
    ('temp_cooling_time', 'Proper cooling time and temperature'),
    ('temp_cold_holding', 'Proper cold holding temperature'),
    ('temp_hot_holding', 'Proper hot holding temperature'),
    ('temp_cooking_time', 'Proper cooking time and temperature'),
    ('reheating_procedure', 'Proper reheating procedure for hot holding'),
    ('time_as_health_control', 'Time as a public health control; procedures and records'),
    ('approved_source', 'Food from an approved source, in good condition, safe and unadulterated'),
    ('food_received_at_temp', 'Food received at proper temperature'),
    ('food_separated_protected', 'Food separated and protected'),
    ('food_surfaces_returnables', 'Food-contact surfaces and returnables cleaned and sanitized'),
    ('condition_of', 'Proper disposition of returned, previously served or reconditioned food'),
    ('knowledge_and_reporting', 'Management and food employees: knowledge, responsibilities and reporting'),
    ('proper_restriction_exclusion', 'Proper use of restriction and exclusion of ill employees'),
    ('hands_clean_washed', 'Hands cleaned and properly washed; gloves used properly'),
    ('no_bare_hands', 'No bare hand contact with ready-to-eat food'),
    ('pastuerized_foods', 'Pasteurized foods used; prohibited food not offered'),
    ('food_additives', 'Food additives approved and properly stored; produce washed'),
    ('toxic_substances', 'Toxic substances properly identified, stored and used'),
    ('approved_water_source', 'Water from an approved source; plumbing installed; backflow protected'),
    ('approved_sewage', ''),
    ('certified_food_manager', 'Person in charge present; demonstration of knowledge; certified food manager'),
    ('food_handler_cards', 'Food handler certification; no unauthorized persons'),
    ('available_hot_cold_water', 'Hot and cold water available at adequate pressure'),
    ('available_records', 'Required records available (shellstock tags, parasite destruction)'),
    ('compliance_with_variance', 'Compliance with variance, specialized process or HACCP plan'),
    ('consumer_advisories', 'Consumer advisories posted for raw or undercooked food and allergens'),
    ('proper_cooling_method', 'Proper cooling method; adequate equipment to maintain temperature'),
    ('proper_date_marking', 'Proper date marking and disposition'),
    ('thermometers_accurate', 'Thermometers provided, accurate and calibrated; test strips available'),
    ('food_establishment_permit', 'Food establishment permit current and posted'),
    ('handwashing_facilities', 'Handwashing facilities adequate, accessible and supplied'),
    ('cleanable_surfaces', 'Food and non-food contact surfaces cleanable, properly designed and used'),
    ('warewashing_facilities', 'Warewashing facilities installed, maintained and used; service sink'),
    ('no_insect_contamination', ''),
    ('personal_cleanliness', 'Personal cleanliness; eating, drinking or tobacco use'),
    ('wiping_cloths', 'Wiping cloths properly used and stored'),
    ('environmental_contamination', 'Environmental contamination prevented'),
    ('approved_thawing', 'Approved thawing method'),
    ('utensils_and_linens', 'Utensils, equipment and linens properly used, stored and handled'),
    ('single_service_use', 'Single-service and single-use articles properly stored and used'),
    ('original_container_labeling', 'Original container labeling of bulk food'),
    ('nonfood_surfaces_clean', 'Non-food contact surfaces clean'),
    ('adequate_ventilation_lighting', 'Adequate ventilation and lighting; designated areas used'),
    ('garbage_and_refuse', 'Garbage and refuse properly disposed; facilities maintained'),
    ('physical_facilities_maintained', 'Physical facilities installed, maintained and clean'),
    ('toilet_facilities', 'Toilet facilities properly constructed, supplied and cleaned'),
    ('other_violations', 'Other violations'),
)
# A cited item whose observation the report does not spell out (about half
# of the cited items in the 2026-10 sample use a layout the parser cannot
# attribute) ships with a neutral phrase: never a bare pest or sewage title.
MCK_NO_OBSERVATION = 'cited on the inspection report; no observation recorded'
MCK_NO_OBSERVATION_TITLES = {
    20: 'Plumbing and wastewater disposal item',
    34: 'Pest control item',
}
MCK_OUT_FIELDS = ','.join(
    ['OBJECTID', 'inspection_number', 'actual_end_date', 'link_number', 'link_type',
     'permitted_name', 'status', 'inspection_type', 'purpose_of_inspection',
     'main_address', 'main_address2', 'total_score', 'custom_comments', 'POINT_X', 'POINT_Y']
    + [col for col, _ in MCK_ITEMS])

# custom_comments layouts seen (inspector-specific), all read below: a
# 'Violations:' header followed by '#9. text', '#9: text', '#42:text',
# '#9 text' or '9 - text' lines, with 'A)' / '\tB)' sub-observations and
# wrapped continuation lines; the same lines under an 'Observations:' header
# or after a repeated 'Temperatures:' header instead (20 of 329 reports with
# cited items in the 90-day sample); no header at all, '45 - text' lines
# after the temperature notes, with or without a blank line between; two
# items pasted on one line ('... supplied. #34. Observed ...');
# 'Temperatures:' / 'Observations:' blocks whose '38 - Ambient WIC' notes
# look like items; and a 'Comments:' / 'COMMENTS:' tail of general remarks.
_MCK_SECTION_RE = re.compile(r'^\s*([A-Za-z][A-Za-z /&\-]{1,30}?)\s*:\s*$')
# '#9. text' / '#9: text' / '#9 text' (a hash alone marks an item) or
# '9 - text' / '9. text' / '9) text' (without a hash the separator is
# required, so '9/26', the pest-control date, is not one, and the text must
# start with a letter, so '12-26' is not either). Up to three digits so that
# a '185 - Beans HH' temperature note is recognised as a note.
_MCK_ITEM_LINE_RE = re.compile(
    r'^\s*(?:(?P<hash>#)\s*(?P<n1>\d{1,3})\b\s*[.:)\-]?|(?P<n2>\d{1,3})\s*(?:[.:)]|-)(?=\s*[A-Za-z(\'"]|\s*$))'
    r'\s*(?P<text>.*?)\s*$')
_MCK_INLINE_ITEM_RE = re.compile(r'(?<=[.;!?])\s+(?=#\s*\d{1,2}\b)')
_MCK_SUBITEM_RE = re.compile(r'^\s*[A-Za-z]\)\s*(.*?)\s*$')
# A sentence: a word closed by sentence punctuation. Temperature notes
# ('38 - Ambient WIC', '185 - Beans HH') carry none; observations do, or
# start with 'Observed'.
_MCK_SENTENCE_RE = re.compile(r'[A-Za-z)][.;!?](?:\s|$)')
# Report-header lines: the certified food manager's or person in charge's
# personal name, the pest contractor, temperature notes. A header-shaped
# line (no sentence in it: 'CFM: Jane Doe 04/26', 'PIC: John Roe', 'Pest
# Control: Acme 10/26') ends any open item block and is never part of one;
# a continuation sentence that happens to start with one of these words
# ('Pest control last treated 10/1; live roaches seen in dish area.') stays
# with its item.
_MCK_HEADER_LINE_RE = re.compile(
    r'^\s*(?:cfm|cfh|certified food manager|pic|person in charge|manager|owner|operator|contact|'
    r'phone|e-?mail|inspector|pest(?: control)?|temperatures?|temps?|observations?)\b', re.I)
_MCK_VIOLATION_SECTIONS = ('violation', 'violations', 'violations observed', 'observed violations')
_MCK_END_SECTIONS = ('comment', 'comments', 'notes', 'note', 'general comments')
# Items whose text decides a text-forced F. When the inspector cited one and
# the parser could not place its sentence (an unread layout, a sentence
# pasted under another item or into the comments tail), the report's own
# pest / sewage sentences are attached to it, so a cited pest item never
# hides the roach it was cited for behind the neutral phrase. Only a CITED
# item gets them: a pest mention on a report whose inspector cited no pest
# item is left to the regex on the line it sits in.
MCK_HAZARD_ITEMS = {20: SEWAGE_ISSUE_RE, 34: PEST_EVIDENCE_RE}


def _mck_observation_like(text):
    """An item line's text is an observation, not a temperature note, when it
    starts with 'Observed' or contains a sentence."""
    return text[:8].lower().startswith('observ') or bool(_MCK_SENTENCE_RE.search(text))


def _mck_violation_blocks(comments):
    """{item number: [observation, ...]} parsed from a layer-4 custom_comments
    value. With a 'Violations:' header the lines after it are the violation
    zone, up to a 'Comments:' header; without one, every line outside a
    named section such as 'Temperatures:' / 'Observations:' is. Inside such
    a section a line is still read as an item when it carries a hash
    ('#10. Observed dirty can opener.') or reads as an observation
    ('34 - Observed live roaches ...'); only its '38 - Ambient WIC' notes are
    left out, and such a note ends an open block instead of joining it.
    Header-shaped lines (the certified food manager's personal name, the
    person in charge, the pest-control contractor) are never part of a
    block."""
    lines = []
    for raw in (comments or '').replace('\r', '').split('\n'):
        lines.extend(_MCK_INLINE_ITEM_RE.split(raw))    # '... supplied. #34. Observed ...'
    sections = [(_MCK_SECTION_RE.match(l).group(1).strip().lower() if _MCK_SECTION_RE.match(l) else None)
                for l in lines]
    has_violation_header = any(s in _MCK_VIOLATION_SECTIONS for s in sections if s)
    blocks = defaultdict(list)
    zone = not has_violation_header      # plain '9 - text' lines allowed right away?
    current = None                       # (item number, [text parts])

    def close():
        nonlocal current
        if current and any(p for p in current[1]):
            blocks[current[0]].append(' '.join(' '.join(p.split()) for p in current[1] if p.strip()))
        current = None

    for line, section in zip(lines, sections):
        if section is not None:
            close()
            if section in _MCK_VIOLATION_SECTIONS:
                zone = True
            elif section in _MCK_END_SECTIONS:
                zone = False
                if has_violation_header:
                    break
            else:
                zone = False             # 'Temperatures:' / 'Observations:' / 'CFM:' block
            continue
        if not line.strip():
            close()
            if not has_violation_header:
                zone = True              # a blank line ends such a block
            continue
        if _MCK_HEADER_LINE_RE.match(line) and not _MCK_SENTENCE_RE.search(line):
            close()
            continue
        m = _MCK_ITEM_LINE_RE.match(line)
        if m:
            number, text = int(m.group('n1') or m.group('n2')), m.group('text')
            close()                      # an item line, or a temperature note shaped like one
            if 1 <= number <= len(MCK_ITEMS) and (zone or m.group('hash') or _mck_observation_like(text)):
                first = _MCK_SUBITEM_RE.match(text)
                current = (number, [first.group(1) if first else text])
            continue
        if current is None:
            continue
        sub = _MCK_SUBITEM_RE.match(line)
        current[1].append(sub.group(1) if sub else line.strip())
    close()
    return blocks


def _mck_hazard_sentences(comments, hazard_re):
    """The report's sentences that carry the hazard (item markers removed,
    scrubbed, deduplicated), in report order."""
    out = []
    for raw in (comments or '').replace('\r', '').split('\n'):
        for s in re.split(r'(?<=[.;!?])\s+', raw):
            m = _MCK_ITEM_LINE_RE.match(s)
            if m:
                s = m.group('text')
            sub = _MCK_SUBITEM_RE.match(s)
            if sub:
                s = sub.group(1)
            s = _dfw_hole_scrub(s)
            if s and hazard_re.search(s) and s not in out:
                out.append(s)
    return out


def _mck_violations(attrs):
    """[category, severity, text] for every item the inspection scored,
    tiered by the points the city assessed (3 priority, 2 priority
    foundation, 1 core), with the inspector's observation for that item. A
    cited pest or sewage item (MCK_HAZARD_ITEMS) whose own text carries no
    hazard sentence gets the report's, wherever the inspector wrote them."""
    comments = attrs.get('custom_comments')
    blocks = _mck_violation_blocks(comments)
    violations = []
    for number, (col, title) in enumerate(MCK_ITEMS, start=1):
        try:
            pts = int(attrs.get(col) or 0)
        except (TypeError, ValueError):
            pts = 0
        if pts <= 0:
            continue
        sev = 'priority' if pts >= 3 else 'priority_foundation' if pts == 2 else 'core'
        texts = [_dfw_hole_scrub(t) for t in blocks.get(number, [])]
        texts = [t for t in texts if t]
        hazard_re = MCK_HAZARD_ITEMS.get(number)
        if hazard_re and not any(hazard_re.search(t) for t in texts):
            texts.extend(s for s in _mck_hazard_sentences(comments, hazard_re)
                         if not any(s in t for t in texts))
        if texts:
            body = ' | '.join(texts)
            desc = f'{title}: {body}' if title else body
        else:
            desc = f'{MCK_NO_OBSERVATION_TITLES.get(number, title)} {MCK_NO_OBSERVATION}'
        cat, _, _ = classify_violation(desc)
        violations.append([cat, sev, desc[:500]])
    return violations


def _mck_clean_name(raw):
    """permitted_name with the layer's artefacts removed: newlines and the
    same name repeated several times ('ASIAN BUFFET\\n\\nASIAN BUFFET...')."""
    seen = []
    for part in (raw or '').replace('\r', '').split('\n'):
        part = ' '.join(part.split())
        if part and part not in seen:
            seen.append(part)
    return _dfw_hole_title(' '.join(seen))


_MCK_CITY_RE = re.compile(r'^\s*([^,]+?)\s*,?\s*([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\s*$')


def _mck_city_state_zip(addr2):
    """'MCKINNEY, TX 75069' -> ('Mckinney', 'TX', '75069'): the plain .title()
    label every fetcher writes. The McKinney-addressed rows the Plano portal
    already put in D1 and data.js are 'Mckinney', and the cities index, the
    city= filter and the per-city embed floor all compare the label
    byte-for-byte, so 'McKinney' would split the town in two."""
    m = _MCK_CITY_RE.match(addr2 or '')
    if not m:
        return 'Mckinney', 'TX', ''
    return _dfw_hole_title(m.group(1)), m.group(2).upper(), m.group(3)


def _mck_point(geometry, attrs):
    """(lon, lat) from a feature's WGS84 geometry (outSR 4326) or its
    POINT_X/POINT_Y strings; (None, None) when the layer has neither."""
    if isinstance(geometry, dict):
        lon, lat = _dfw_hole_coord(geometry.get('x')), _dfw_hole_coord(geometry.get('y'))
        if lon is not None and lat is not None:
            return lon, lat
    lon, lat = _dfw_hole_coord(attrs.get('POINT_X')), _dfw_hole_coord(attrs.get('POINT_Y'))
    if lon is not None and lat is not None:
        return lon, lat
    return None, None


def _mck_count(where):
    data = _dfw_hole_get(MCK_INSPECTIONS_LAYER + '/query',
                         params={'where': where, 'returnCountOnly': 'true', 'f': 'json'},
                         label='McKinney count')
    if 'error' in data:
        raise RuntimeError(f"McKinney count: ArcGIS error {data['error'].get('message')}")
    try:
        return int(data['count'])
    except (KeyError, TypeError, ValueError):
        raise RuntimeError(f'McKinney count: no count in {str(data)[:120]}')


def _mck_newest():
    """The layer's newest inspection date ('' when the probe fails; the lag
    note is then simply not left)."""
    try:
        data = _dfw_hole_get(MCK_INSPECTIONS_LAYER + '/query', params={
            'where': '1=1', 'outFields': 'actual_end_date',
            'orderByFields': 'actual_end_date DESC', 'resultRecordCount': 1, 'f': 'json',
        }, label='McKinney newest')
        ts = data['features'][0]['attributes']['actual_end_date']
        return _mck_date(ts) or ''
    except Exception as e:
        log.debug(f'McKinney: newest-row probe failed: {e}')
        return ''


def _mck_date(ts):
    """Epoch ms (local wall-clock flagged UTC) -> 'YYYY-MM-DD' or None."""
    try:
        d = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return sane_inspection_date(d)


def _mck_permits():
    """Layer 0 by link_number: {'point': (lon, lat) | None, 'name', 'class'}.
    An unreadable layer RAISES like an unreadable inspections layer (the same
    host serves all five): shipping without it would put mobile units and
    splash pads on the map for good (D1 never deletes a restaurant) and
    drop the rows whose only name is the permit's without a hold."""
    try:
        rows = _arcgis_query_all(MCK_PERMITS_LAYER,
                                 out_fields='link_number,description,WORK_CLASS,POINT_X,POINT_Y',
                                 page_size=MCK_PAGE)
    except Exception as e:
        raise RuntimeError(f'McKinney: permits layer unreadable ({e}); nothing shipped') from e
    permits = {}
    for f in rows:
        a = f.get('attributes') or {}
        link = (a.get('link_number') or '').strip()
        if not link:
            continue
        lon, lat = _mck_point(f.get('geometry'), a)
        permits[link] = {'point': (lon, lat) if lon is not None else None,
                         'name': _mck_clean_name(a.get('description')),
                         'class': (a.get('WORK_CLASS') or '').strip()}
    return permits


def _mck_non_restaurant_permits():
    """link_numbers on the mobile/temporary, farmers-market and school layers.
    A layer that cannot be read RAISES: a school cafeteria that ships once
    stays on the map for good, so the window is re-read instead."""
    links = set()
    for layer in MCK_NON_RESTAURANT_LAYERS:
        try:
            rows = _arcgis_query_all(f'{MCK_BASE}/{layer}', out_fields='link_number', page_size=MCK_PAGE)
        except Exception as e:
            raise RuntimeError(f'McKinney: permit layer {layer} unreadable ({e}); nothing shipped') from e
        links.update((f.get('attributes') or {}).get('link_number') or '' for f in rows)
    links.discard('')
    return links


def fetch_mckinney(since_date=None, limit=None):
    """
    Fetch McKinney, TX food inspections from the city's ArcGIS MapServer
    (see the section notes). One record per inspection completed on or after
    since_date (test mode, no since: the last MCK_TEST_WINDOW_DAYS; no since
    and no limit: the layer's 2017 start, which main() never asks for, its
    full mode starting 2024-01-01). The window is read with a server-side
    count check and raises on any shortfall or request failure, including
    an unreadable permit layer; nothing partial ships.

    Scoring: the city publishes demerit points, not a 0-100 score, so
    original_score is the demerit total and risk_score is the TX form's
    score, 100 - demerits (Richardson, Dallas and Plano ship the same
    number); each cited item is tiered by the points the city assessed
    (3 = priority, 2 = priority foundation, 1 = core; TX form weights,
    verified live) and the tiers must add up to the total: an inspection
    where they do not is dropped and held (MCK_SOURCE, key = its number),
    and MCK_MISMATCH_MIN of them making up more than MCK_MISMATCH_MAX_SHARE
    of the window raise. results carries the city's total and status.

    Scope: voided inspections and pre-opening walk-throughs are skipped;
    permits on the mobile/temporary, farmers-market and school layers, the
    layer-0 work classes in MCK_NON_RESTAURANT_CLASSES and school/daycare/
    micro-market names (MCK_SCHOOL_NAME_RE) are dropped. The same inspection
    appears once per PDF attachment in the layer; rows are deduplicated on
    inspection_number.
    """
    log.info(f"Fetching McKinney data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else None
    if not since:
        since = ((datetime.now() - timedelta(days=MCK_TEST_WINDOW_DAYS)).strftime('%Y-%m-%d')
                 if limit else MCK_HISTORY_START)
    where = f"actual_end_date >= DATE '{since}'"
    expected = _mck_count(where)
    try:
        rows = _arcgis_query_all(MCK_INSPECTIONS_LAYER, where=where,
                                 out_fields=MCK_OUT_FIELDS, page_size=MCK_PAGE)
    except Exception as e:
        raise RuntimeError(f'McKinney: inspections layer unreadable ({e}); nothing shipped') from e
    if len(rows) < expected:
        raise RuntimeError(f'McKinney: read {len(rows)} of {expected} inspections since {since}; '
                           f'not shipping a partial window')
    log.info(f"  McKinney: {len(rows)} inspection rows since {since} (server count {expected})")

    by_number = {}
    for f in rows:
        a = f.get('attributes') or {}
        number = (a.get('inspection_number') or '').strip()
        if number and number not in by_number:
            by_number[number] = f
    permits = _mck_permits() if by_number else {}
    non_restaurant = _mck_non_restaurant_permits() if by_number else set()

    results, newest, skipped, mismatched = [], '', defaultdict(int), []
    for number, f in by_number.items():
        a = f.get('attributes') or {}
        insp_date = _mck_date(a.get('actual_end_date'))
        if not insp_date:
            skipped['no date'] += 1
            continue
        newest = max(newest, insp_date)
        if insp_date < since:
            continue
        status = (a.get('status') or '').strip()
        purpose = (a.get('purpose_of_inspection') or '').strip()
        if status in MCK_SKIP_STATUSES or purpose in MCK_SKIP_PURPOSES:
            skipped[status if status in MCK_SKIP_STATUSES else purpose] += 1
            continue
        link = (a.get('link_number') or '').strip()
        permit = permits.get(link) or {}
        name = _mck_clean_name(a.get('permitted_name')) or permit.get('name') or ''
        if not name:
            skipped['no name'] += 1
            continue
        if (link in non_restaurant or permit.get('class') in MCK_NON_RESTAURANT_CLASSES
                or MCK_SCHOOL_NAME_RE.search(name)):
            skipped['not a fixed-address food establishment'] += 1
            continue
        try:
            demerits = max(0, int(a.get('total_score') or 0))
        except (TypeError, ValueError):
            demerits = 0
        violations = _mck_violations(a)
        _, pv, pfv, cv = calc_risk_score(violations)
        if demerits != 3 * pv + 2 * pfv + cv:
            mismatched.append((number, insp_date, demerits, (pv, pfv, cv)))
            continue
        lon, lat = _mck_point(f.get('geometry'), a)
        if lon is None and permit.get('point'):
            lon, lat = permit['point']
        city, state, zip_code = _mck_city_state_zip(a.get('main_address2'))
        results.append({
            'name': name,
            'address': _dfw_hole_title(a.get('main_address')),
            'city': city,
            'state': state,
            'zip': zip_code,
            'latitude': lat,
            'longitude': lon,
            'inspection_date': insp_date,
            'original_score': demerits,                      # the city's demerit total (lower is better)
            'risk_score': max(0, min(100, 100 - demerits)),  # the TX form score, as Richardson/Dallas/Plano
            'priority_violations': pv,
            'priority_foundation_violations': pfv,
            'core_violations': cv,
            'total_violations': len(violations),
            'violations': violations,
            'inspection_type': re.sub(r'^\d+\s*-\s*', '', purpose),
            'results': f'{demerits} demerits' + (f' ({status})' if status else ''),
            'source': MCK_SOURCE,
            'source_url': MCK_INSPECTIONS_LAYER + '/query?' + urlencode(
                {'where': f"inspection_number='{number}'", 'outFields': '*', 'f': 'html'}),
            'source_id': f'mck_{number}',
            'metro': 'DFW',
        })
    if skipped:
        log.info(f"  McKinney: skipped {dict(skipped)}")
    if mismatched:
        scored = len(results) + len(mismatched)
        if len(mismatched) >= MCK_MISMATCH_MIN and len(mismatched) > MCK_MISMATCH_MAX_SHARE * scored:
            raise RuntimeError(
                f'McKinney: {len(mismatched)} of {scored} inspections have item points that do not add '
                f'up to total_score (first {mismatched[0][0]}: {mismatched[0][2]} demerits, P/PF/C '
                f'{mismatched[0][3]}); the item columns changed, not shipping')
        log.warning(f"  McKinney: {len(mismatched)} inspection(s) dropped and held, item points do not "
                    f"add up to total_score: {', '.join(m[0] for m in mismatched[:5])}")
        for number, d, _, _ in mismatched:
            _hold_freshness(MCK_SOURCE, d, counted=True, key=number)
    truncated = bool(limit) and len(results) > limit
    results.sort(key=lambda r: (r['inspection_date'], r['risk_score'], r['source_id']))
    if truncated:
        results = results[-limit:]              # the newest `limit` inspections
    if not truncated:
        _note_source_lag('McKinney', newest or _mck_newest())
    log.info(f"McKinney: {len(results)} inspections (source newest {newest or 'n/a'})")
    return results


# ── Lewisville ──────────────────────────────────────────────────────────────
# The city's JSON web service (query.cityoflewisville.com/v2, behind the Vue
# app at eservices.cityoflewisville.com/healthinspectionscores/) returns
# every inspection in a date window in one answer: InspectionID,
# PermitNumber, InspectionDate, BusinessName, BusinessAddress, Score (the
# official 0-100 score as a string; 'NO SCORE' and 'PASS' for unscored
# visits), Lat/Lng. ~25 inspections a week, no violations, no inspection
# type, and only the trailing 365 days are kept (a window that starts
# earlier returns those 365 days, verified live). TLS: the host sends only
# its leaf certificate, without the "Go Daddy Secure Certificate Authority
# - G2" intermediate, so the default trust store rejects it; requests are
# verified against LEW_CA_BUNDLE (certifi's roots plus that published
# intermediate, provenance in the file header) for this host only.
# Verification is never disabled.
LEW_API = 'https://query.cityoflewisville.com/v2/'
LEW_SERVICE = 'Health/HealthInspectionScores_byDate'
LEW_APP_URL = 'https://eservices.cityoflewisville.com/healthinspectionscores/'
LEW_RETENTION_DAYS = 365
LEW_CA_BUNDLE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'data', 'certs', 'lewisville_ca_bundle.pem')
LEW_SOURCE = 'Lewisville Environmental Health'


def _lew_verify():
    """The CA bundle path for the Lewisville host, or True (the default
    store, which fails today) when the bundle is missing from the checkout."""
    if os.path.exists(LEW_CA_BUNDLE):
        return LEW_CA_BUNDLE
    log.warning(f'Lewisville: CA bundle {LEW_CA_BUNDLE} missing; using the default trust store')
    return True


def _lew_date(value):
    """'2026-11-02T13:44:00.000Z' is local wall-clock time with a 'Z' pasted
    on: the date part is the inspection's local date."""
    return sane_inspection_date(str(value or '')[:10])


def fetch_lewisville(since_date=None, limit=None):
    """
    Fetch Lewisville, TX inspection scores from the city's web service (see
    the section notes). One GET covers the window since_date .. tomorrow;
    the start is floored at today - LEW_RETENTION_DAYS because the service
    keeps nothing older. The answer must be JSON with a 'projects' list and
    no error marker ({"projects": [], "BusList": [{"Error_ID": ...}]}),
    otherwise the fetch raises and nothing ships.

    Scoring: the official 0-100 score is both original_score and risk_score;
    there are no violation lines. Rows whose Score is not a positive number
    ('NO SCORE', 'PASS': unscored visits, 19 of 1146 in a year, among them
    the mobile stands and a few restaurants) are dropped rather than shipped
    as a grade; a window of DFW_HOLE_UNSCORED_MIN rows that is mostly
    unscored raises instead (a renamed or re-typed Score column). School
    and child-care kitchens (LISD cafeterias, daycares) are dropped by name
    (DFW_HOLE_NON_RESTAURANT_RE); nursing homes, park concessions and golf
    courses are kept.
    """
    log.info(f"Fetching Lewisville data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else ''
    floor = (datetime.now() - timedelta(days=LEW_RETENTION_DAYS)).strftime('%Y-%m-%d')
    if not since or since < floor:
        since = floor
    end = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'{LEW_API}?webservice={LEW_SERVICE}&DateBegin={since}&DateEnd={end}'
    payload = _dfw_hole_get(url, label='Lewisville', verify=_lew_verify())
    if not isinstance(payload, dict) or not isinstance(payload.get('projects'), list):
        raise RuntimeError(f'Lewisville: unexpected answer shape {str(payload)[:120]}')
    errors = payload.get('BusList')
    if errors:
        raise RuntimeError(f'Lewisville: the web service reported an error: {str(errors)[:120]}')
    rows = payload['projects']
    log.info(f"  Lewisville: {len(rows)} rows for {since} .. {end}")

    results, seen, newest, in_window, unscored, schools = [], set(), '', 0, 0, 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        insp_id = row.get('InspectionID')
        if insp_id in (None, '') or insp_id in seen:
            continue
        seen.add(insp_id)
        insp_date = _lew_date(row.get('InspectionDate'))
        if not insp_date:
            continue
        newest = max(newest, insp_date)
        if insp_date < since:
            continue
        in_window += 1
        try:
            score = int(str(row.get('Score') or '').strip())
        except ValueError:
            unscored += 1
            continue
        if score <= 0:
            unscored += 1
            continue
        name = _dfw_hole_title(row.get('BusinessName'))
        if not name:
            continue
        if DFW_HOLE_NON_RESTAURANT_RE.search(name):
            schools += 1
            continue
        results.append({
            'name': name,
            'address': _dfw_hole_title(row.get('BusinessAddress')),
            'city': 'Lewisville',
            'state': 'TX',
            'zip': '',
            'latitude': _dfw_hole_coord(row.get('Lat')),
            'longitude': _dfw_hole_coord(row.get('Lng')),
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),   # the city's official 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': '',
            'source': LEW_SOURCE,
            'source_url': LEW_APP_URL,
            'source_id': f'lew_{insp_id}',
            'metro': 'DFW',
        })
    if in_window >= DFW_HOLE_UNSCORED_MIN and unscored * 2 > in_window:
        raise RuntimeError(f'Lewisville: {unscored} of {in_window} rows since {since} carry no usable '
                           f'Score (a format change?); not shipping')
    if unscored or schools:
        log.info(f"  Lewisville: {unscored} unscored rows ('NO SCORE' / 'PASS') and "
                 f"{schools} school / child-care rows dropped")
    truncated = bool(limit) and len(results) > limit
    results.sort(key=lambda r: (r['inspection_date'], r['risk_score'], r['source_id']))
    if truncated:
        results = results[-limit:]
    if not truncated:
        _note_source_lag('Lewisville', newest)
    log.info(f"Lewisville: {len(results)} inspections (source newest {newest or 'n/a'})")
    return results


# ── Denton ──────────────────────────────────────────────────────────────────
# CKAN open-data portal (data.cityofdenton.com) with a DataStore over the
# daily FME export 'fme_latestrestauranthealthscores.xlsx' (~11:50 UTC):
# scores and results only, history to 2010, up to nine dates per permit.
# The export repeats every row ~100 times (376k rows for ~3,100 distinct
# permit+date pairs), so every query is DISTINCT; HealthScoreDate is a text
# column mixing 'YYYYMMDD' and 'YYYYMMDDHHMMSS', which string comparison
# against 'YYYYMMDD' filters correctly; ~37% of rows carry no coordinates.
# Cloudflare's WAF answers 403 (HTML) to some SQL shapes (a SUBSTR() call
# did), so the queries below keep to the two shapes verified live: COUNT over
# a DISTINCT subquery, and SELECT DISTINCT ... WHERE ... ORDER BY <every
# column> ... LIMIT/OFFSET (the order is total: a fifth of the permit+date
# pairs have several distinct rows, which a partial order could shuffle
# across a page boundary). An HTML or non-200 answer raises (blocked),
# never ships. HealthScoreLetter uses Denton's own cutoffs (A 94-100,
# B 87-93, C 80-86, F 0-79) and is never copied: the grade is DineScores'
# from the score.
DEN_API = 'https://data.cityofdenton.com/api/3/action/'
DEN_RESOURCE = 'b596e52a-0f84-4766-b376-be9e3ee0b6df'
DEN_DATASET_URL = 'https://data.cityofdenton.com/dataset/restaurant-health-scores'
DEN_HISTORY_START = '2010-01-01'          # the data starts 2010-05-25
DEN_PAGE = 2000
DEN_SOURCE = 'Denton Open Data'
DEN_COLUMNS = ('PERMIT_NO', 'Name', 'Address', 'Type', 'Lat', 'Long', 'RESULT',
               'HealthScoreInt', 'HealthScoreLetter', 'HealthScoreDate')
# Newest first, then every other column: a total order, so LIMIT/OFFSET pages
# never shuffle the rows of one permit+date pair.
DEN_ORDER_BY = 'ORDER BY "HealthScoreDate" DESC, ' + ', '.join(
    f'"{c}"' for c in DEN_COLUMNS if c != 'HealthScoreDate')
DEN_RESULT_TEXT = {
    'APPROVED': 'Approved', 'APP W/COND': 'Approved with conditions', 'FAILED': 'Failed',
    'FAILED W/FEE': 'Failed (re-inspection fee)', 'CHECK NOTES': 'Check notes', 'PASSED': 'Passed',
}


def _den_sql(sql):
    """Run one DataStore SQL statement; the result rows (raise on anything
    but a successful JSON answer)."""
    payload = _dfw_hole_get(DEN_API + 'datastore_search_sql', params={'sql': sql}, label='Denton')
    if not isinstance(payload, dict) or not payload.get('success') \
            or not isinstance(payload.get('result'), dict):
        raise RuntimeError(f'Denton: DataStore answered without a result: {str(payload)[:160]}')
    records = payload['result'].get('records')
    if not isinstance(records, list):
        raise RuntimeError('Denton: DataStore result carries no records list')
    return records


def _den_date(value):
    """'20261028000000' / '20240521' -> 'YYYY-MM-DD' or None."""
    s = str(value or '').strip()
    if len(s) < 8 or not s[:8].isdigit():
        return None
    return sane_inspection_date(f'{s[:4]}-{s[4:6]}-{s[6:8]}')


def _den_result_text(value):
    v = ' '.join(str(value or '').split())
    return DEN_RESULT_TEXT.get(v.upper(), v.title()) if v else ''


def fetch_denton(since_date=None, limit=None):
    """
    Fetch Denton, TX restaurant health scores from the city's CKAN DataStore
    (see the section notes). The window since_date .. now is read as
    DISTINCT rows newest-first in DEN_PAGE pages until a short page, then
    checked against a server-side count of distinct permit+date pairs; a
    shortfall or any request failure raises and nothing ships. With a
    `limit` the read stops once the limit is covered (a bounded read: the
    count is not checked and no lag note is left).

    Scoring: HealthScoreInt is the official 0-100 score (original_score and
    risk_score); no violation text exists. Rows without a permit number, a
    valid date or a positive score are skipped (a window of
    DFW_HOLE_UNSCORED_MIN rows that is mostly unscored raises instead); the
    city's RESULT text goes to results. One record per permit per date (the
    lowest score wins when the export disagrees with itself). Every permit
    type in the data is a food establishment (restaurants, bars, groceries,
    convenience stores), so nothing is filtered by type; school and
    child-care names (DFW_HOLE_NON_RESTAURANT_RE, none in the data today)
    are dropped like everywhere else in the metro, and the mobile units the
    names mention all carry a fixed address, so they are kept.
    """
    log.info(f"Fetching Denton data (since={since_date}, limit={limit})")
    since = str(since_date)[:10] if since_date else DEN_HISTORY_START
    where = f'"HealthScoreDate" >= \'{since.replace("-", "")}\''
    cols = ','.join(f'"{c}"' for c in DEN_COLUMNS)
    count_rows = _den_sql(f'SELECT COUNT(*) AS n FROM (SELECT DISTINCT "PERMIT_NO",'
                          f'"HealthScoreDate" FROM "{DEN_RESOURCE}" WHERE {where}) t')
    try:
        expected = int(count_rows[0]['n'])
    except (IndexError, KeyError, TypeError, ValueError):
        raise RuntimeError(f'Denton: no count in {str(count_rows)[:120]}')

    rows, offset, complete = [], 0, False
    while True:
        page = _den_sql(f'SELECT DISTINCT {cols} FROM "{DEN_RESOURCE}" WHERE {where} '
                        f'{DEN_ORDER_BY} LIMIT {DEN_PAGE} OFFSET {offset}')
        rows.extend(r for r in page if isinstance(r, dict))
        if len(page) < DEN_PAGE:
            complete = True
            break
        if limit and len({(r.get('PERMIT_NO'), str(r.get('HealthScoreDate') or '')[:8])
                          for r in rows}) >= limit:
            break                                  # a bounded read; the rest is not needed
        offset += len(page)
        time.sleep(DFW_HOLE_PAGE_DELAY)
    if complete and len(rows) < expected:
        raise RuntimeError(f'Denton: read {len(rows)} distinct rows but the DataStore counts '
                           f'{expected} permit+date pairs since {since}; not shipping a partial window')
    log.info(f"  Denton: {len(rows)} distinct rows since {since} (server count {expected})")

    by_key, newest, skipped, in_window = {}, '', defaultdict(int), 0
    for row in rows:
        permit = ' '.join(str(row.get('PERMIT_NO') or '').split())
        insp_date = _den_date(row.get('HealthScoreDate'))
        if not permit or not insp_date:
            skipped['no permit or date'] += 1
            continue
        newest = max(newest, insp_date)
        if insp_date < since:
            continue
        in_window += 1
        try:
            score = int(str(row.get('HealthScoreInt') or '').strip())
        except ValueError:
            skipped['unscored'] += 1
            continue
        if score <= 0:
            skipped['unscored'] += 1
            continue
        name = _dfw_hole_title(row.get('Name'))
        if not name:
            skipped['no name'] += 1
            continue
        if DFW_HOLE_NON_RESTAURANT_RE.search(name):
            skipped['school / child care'] += 1
            continue
        key = (permit, insp_date)
        if key in by_key and by_key[key]['risk_score'] <= score:
            continue                               # keep the worst of a disagreeing export
        by_key[key] = {
            'name': name,
            'address': _dfw_hole_title(row.get('Address')),
            'city': 'Denton',
            'state': 'TX',
            'zip': '',
            'latitude': _dfw_hole_coord(row.get('Lat')),
            'longitude': _dfw_hole_coord(row.get('Long')),
            'inspection_date': insp_date,
            'original_score': score,
            'risk_score': max(0, min(100, score)),   # the city's official 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': _den_result_text(row.get('RESULT')),
            'source': DEN_SOURCE,
            'source_url': DEN_DATASET_URL,
            'source_id': f"den_{permit}_{insp_date.replace('-', '')}",
            'metro': 'DFW',
        }
    unscored = skipped.get('unscored', 0)
    if in_window >= DFW_HOLE_UNSCORED_MIN and unscored * 2 > in_window:
        raise RuntimeError(f"Denton: {unscored} of {in_window} rows since {since} carry no usable "
                           f"HealthScoreInt (a format change?); not shipping")
    if skipped:
        log.info(f"  Denton: skipped {dict(skipped)}")
    results = sorted(by_key.values(), key=lambda r: (r['inspection_date'], r['risk_score'], r['source_id']))
    truncated = bool(limit) and (not complete or len(results) > limit)
    if truncated:
        results = results[-limit:]
    if not truncated:
        _note_source_lag('Denton', newest)
    log.info(f"Denton: {len(results)} inspections (source newest {newest or 'n/a'})")
    return results


def geocode_census_batch(records, benchmark='Public_AR_Current'):
    """
    Batch-geocode US street addresses with the free Census Bureau geocoder
    (up to 10k addresses per request — thousands of times faster than the
    1 req/sec Nominatim loop). Fills latitude/longitude in place and returns
    the number geocoded. `benchmark` picks the address database: the default
    'Public_AR_Current' misses newer streets in fast-growing areas (it
    matched 0 of 81 unresolved Florida Emerald Coast addresses in Oct 2026),
    while 'Public_AR_Census2020' resolves some of those, so the caller runs
    both.
    """
    import csv, io

    # Suite/unit/building tokens confuse the Census matcher — strip them for
    # geocoding only (the record's display address is untouched).
    def match_address(addr):
        cleaned = re.sub(r'\s+(?:ste|suite|unit|bldg|building|fl|floor|rm|#)\.?\s*[\w&-]*\s*$',
                         '', addr or '', flags=re.I)
        return re.sub(r'\s{2,}', ' ', cleaned).strip()

    geocoded = 0
    # 5k-row batches routinely exceed the Census service's comfortable
    # response time and used to blow the request timeout, silently leaving
    # whole cities ungeocoded — smaller chunks + a retry are far sturdier.
    chunk_size = 2000
    for chunk_start in range(0, len(records), chunk_size):
        chunk = records[chunk_start:chunk_start + chunk_size]
        rows = io.StringIO()
        writer = csv.writer(rows)
        for idx, r in enumerate(chunk):
            writer.writerow([idx, match_address(r.get('address', '')), r.get('city', ''),
                             r.get('state', ''), (r.get('zip', '') or '').split('-')[0]])
        resp = None
        for attempt in (1, 2):
            try:
                resp = requests.post(
                    'https://geocoding.geo.census.gov/geocoder/locations/addressbatch',
                    files={'addressFile': ('addresses.csv', rows.getvalue(), 'text/csv')},
                    data={'benchmark': benchmark},
                    timeout=600)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                log.warning(f"Census batch geocode failed (attempt {attempt}): {e}")
                resp = None
                time.sleep(5)
        if resp is None:
            continue
        # Response CSV: id, input, match status, match type, matched addr, "lon,lat", ...
        for row in csv.reader(io.StringIO(resp.text)):
            if len(row) >= 6 and row[2] == 'Match' and ',' in row[5]:
                try:
                    lon, lat = row[5].split(',')
                    rec = chunk[int(row[0])]
                    rec['latitude'] = float(lat)
                    rec['longitude'] = float(lon)
                    geocoded += 1
                except (ValueError, IndexError):
                    continue
    return geocoded


def geocode_missing_coords(records, nominatim_fallback_cap=None):
    """
    Geocode records missing latitude/longitude: Census Bureau batch first,
    then a capped Nominatim fallback for the stragglers (1 req/sec).
    """
    if nominatim_fallback_cap is None:
        nominatim_fallback_cap = NOMINATIM_FALLBACK_CAP
    missing = [r for r in records if not r.get('latitude') or not r.get('longitude')]
    if not missing:
        log.info("All records have coordinates, no geocoding needed")
        return
    log.info(f"Geocoding {len(missing)} records missing coordinates (Census batch)...")
    geocoded = geocode_census_batch(missing)

    still_missing = [r for r in missing if not r.get('latitude') or not r.get('longitude')]
    if still_missing:
        # Second Census pass on the older benchmark: it knows addresses the
        # current one does not (and vice versa), and a batch call is cheap.
        geocoded += geocode_census_batch(still_missing, benchmark='Public_AR_Census2020')
        still_missing = [r for r in missing if not r.get('latitude') or not r.get('longitude')]
    fallback = still_missing[:nominatim_fallback_cap]
    skipped = len(still_missing) - len(fallback)
    for r in fallback:
        lat, lon = geocode_address(r['address'], r['city'], r['state'])
        if lat and lon:
            r['latitude'] = lat
            r['longitude'] = lon
            geocoded += 1
    if skipped > 0:
        log.warning(f"Skipped Nominatim fallback for {skipped} records "
                    f"(cap {nominatim_fallback_cap}); they will retry next run")
    log.info(f"Geocoded {geocoded}/{len(missing)} records")

# ─── SAVE TO JSON (for embedding in app as fallback) ─────────────────────────

def save_to_json(all_inspections, output_path):
    """Save processed inspections to a JSON file for review/embedding."""
    
    # Group by restaurant, keep most recent inspection per restaurant
    restaurants = defaultdict(list)
    for insp in all_inspections:
        rest_id = make_restaurant_id(insp['name'], insp['address'], insp['city'])
        restaurants[rest_id].append(insp)
    
    output = []
    for rest_id, inspections in restaurants.items():
        inspections.sort(key=lambda x: x.get('inspection_date', ''), reverse=True)
        most_recent = inspections[0].copy()
        most_recent['_id'] = rest_id
        most_recent['_inspection_count'] = len(inspections)
        output.append(most_recent)
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    log.info(f"Saved {len(output)} restaurants to {output_path}")
    return output


def write_records_json(all_inspections, output_path):
    """Dump every inspection of this run (the full fetcher dicts, Jev reading
    included) so a backfill can be split across runs or machines and put
    back together with --input-records. Gzipped when the path ends in .gz."""
    opener = gzip.open if str(output_path).endswith('.gz') else open
    with opener(output_path, 'wt', encoding='utf-8') as f:
        json.dump(all_inspections, f, default=str)
    log.info(f"Saved {len(all_inspections)} inspection records to {output_path}")


def read_records_json(paths):
    """Load inspection records written by write_records_json (any number of
    files; a file may be gzipped). Each record must carry the fetcher
    contract's identity and date keys or the file is refused."""
    records = []
    for path in paths:
        opener = gzip.open if str(path).endswith('.gz') else open
        with opener(path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"{path}: expected a JSON list of inspection records")
        for i, rec in enumerate(data):
            if (not isinstance(rec, dict)
                    or not all(isinstance(rec.get(k), str) and rec.get(k) for k in ('name', 'address', 'city', 'state', 'source'))
                    or not sane_inspection_date(rec.get('inspection_date'))
                    or not isinstance(rec.get('risk_score'), (int, float)) or isinstance(rec.get('risk_score'), bool)
                    or not isinstance(rec.get('violations'), list)):
                raise ValueError(f"{path}: record {i} is not a complete inspection record")
        log.info(f"Loaded {len(data)} inspection records from {path}")
        records.extend(data)
    return records


def _load_existing_data_js(path):
    """Load records from an existing data.js file. Returns a list of compact record dicts."""
    try:
        with open(path, 'r') as f:
            content = f.read()
        json_str = content.split('window.DATA = ', 1)[1].rstrip(';\n')
        return json.loads(json_str)
    except Exception as e:
        log.warning(f"Could not load existing data.js from {path}: {e}")
        return []


MAX_HISTORY = 6            # [date, risk_score] pairs kept per restaurant
STALE_MONTHS = 30          # merged records older than this are pruned
EMBED_BUDGET = 55000       # global cap on embedded records: Cloudflare Pages
                           # rejects any asset over 25 MiB, and ~62k compact
                           # records ≈ 24 MB. Completeness comes from D1.
EMBED_CITY_FLOOR = 20      # per-city minimum kept regardless of the global
                           # budget, so every covered town paints instantly
DATA_JS_MAX_BYTES = 24_000_000  # hard cap on the written file. The record
                           # budget above bounds it only indirectly (the
                           # per-city floors grow with every new town), and
                           # Pages rejects any asset over 25 MiB (26,214,400
                           # bytes) by failing the deploy with no error on
                           # the commit; ~2 MB stays free for a week's growth.


def _weighted_from_history(history_pairs):
    """Weighted score from [[date, risk_score], ...] sorted newest-first."""
    weights = [0.6, 0.3, 0.1]
    total = 0.0
    total_w = 0.0
    for idx, (_, score) in enumerate(history_pairs[:3]):
        total += (score or 0) * weights[idx]
        total_w += weights[idx]
    return round(total / total_w) if total_w else 0


def _same_day_rank(insp):
    """Sort key, higher = worse: picks which of a restaurant's inspections on
    one date stands for that date. Several permits often share a name and an
    address (Disneyland's food stands, a grocery's deli and bakery counters),
    so they share a restaurant id; showing the worst keeps a problem at any
    of them visible, and the choice no longer depends on fetch order."""
    rs = insp.get('risk_score')
    rs = rs if isinstance(rs, (int, float)) else 100
    text = build_violation_text(insp.get('violations') or [])
    is_f = calculate_vetted_grade(rs) == 'F' or _text_forces_f(text, insp.get('_jev') or {})
    return (bool(is_f), -rs, insp.get('priority_violations') or 0,
            insp.get('total_violations') or 0, str(insp.get('source_id') or ''))


def _one_per_day(inspections):
    """Newest first, one inspection per date (the worst of that day)."""
    ordered = sorted(inspections, key=lambda i: (i.get('inspection_date') or '', _same_day_rank(i)),
                     reverse=True)
    out, seen = [], set()
    for i in ordered:
        d = i.get('inspection_date') or ''
        if d not in seen:
            seen.add(d)
            out.append(i)
    return out


def _coords_from(chosen, siblings):
    """(lat, lng) of the chosen inspection, else of the first sibling that has
    both: same restaurant id means same name and address."""
    for i in [chosen] + list(siblings):
        if i.get('latitude') is not None and i.get('longitude') is not None:
            return i.get('latitude'), i.get('longitude')
    return chosen.get('latitude'), chosen.get('longitude')


def _text_forces_f(violation_text, jev=None):
    """The text-forced F: Jev's hazard decision when the reader made one for
    this record (`_jev['forces_f']`), else today's regex over the joined text."""
    if jev and jev.get('forces_f') is not None:
        return bool(jev['forces_f'])
    return has_active_pest_or_sewage(violation_text)


def _apply_grades(rec):
    """
    Recompute ws/vg/inf/vs on a compact record from its history + violations.
    The raw violations array ('v') is only present in-memory for records built
    this run — it is stripped before serialization to keep data.js small. For
    records loaded back from an existing data.js, grading text is derived from
    the stored summary verbatims and the existing inf/vs are left untouched.
    """
    has_violations = rec.get('v') is not None
    if has_violations:
        violation_text = build_violation_text(
            [tuple(v) for v in (rec.get('v') or [])])
    else:
        violation_text = '|||'.join(
            s.get('verbatim', '') for s in (rec.get('vs') or []) if s.get('verbatim'))
    rec['ws'] = _weighted_from_history(rec.get('h') or [[rec.get('d', ''), rec.get('rs', 0)]])
    jev = rec.get('_jev') or {}
    vg = calculate_vetted_grade(rec['ws'])
    if has_violations:
        text_f = _text_forces_f(violation_text, jev)
    else:
        # A record kept from an earlier run keeps the text decision of the run
        # that last read its full violation text (Jev's, in decide mode). Its
        # stored verbatims are cut at 200 characters, and re-reading them with
        # today's regex would undo Jev's F's (and bring back the regex's) a
        # week later. A stored F may be evidence-based (pest auto-F): never
        # upgrade it without new inspection data; a fresh fetch of the
        # restaurant re-grades it from full text. Score rules still re-apply.
        text_f = rec.get('vg') == 'F'
    if calculate_vetted_grade(rec.get('rs', 0)) == 'F' or text_f:
        vg = 'F'
    rec['vg'] = vg
    if has_violations:
        rec['inf'] = jev['chips'] if jev.get('chips') is not None else detect_infractions(violation_text)
        if jev.get('summaries') is not None:
            summaries = [dict(s) for s in jev['summaries']]
        else:
            summaries = summarize_violations(violation_text)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]
        rec['vs'] = summaries


def _embed_trim(final, by_city, drop_count):
    """Drop about drop_count records from final so that every (city, state)
    keeps the same FRACTION of its newest records, never fewer than
    EMBED_CITY_FLOOR (or all of them for a town below the floor). by_city
    maps each city to its records newest-first; final is the embedded
    subset. Returns the kept records in their current order.

    A common fraction keeps the embedded map an unbiased sample of every
    city's true density: Houston keeps ~60% of its 1,000 and a 30-restaurant
    town its floor. Levelling every city to the same COUNT instead gave each
    metro ~158 records once 3,900 towns were in the file, and dropping the
    globally oldest records gave them even less (measured 2026-11-03)."""
    keep_ids = {id(r) for r in final}
    pools = {}
    for key, city_recs in by_city.items():
        recs = [r for r in city_recs if id(r) in keep_ids]
        if recs:
            pools[key] = recs
    sizes = [len(recs) for recs in pools.values()]
    target = len(final) - drop_count

    def kept(fraction):
        return sum(max(min(n, EMBED_CITY_FLOOR), int(n * fraction)) for n in sizes)

    if kept(0.0) >= target:
        fraction = 0.0           # the floors alone fill the budget
    else:
        lo, hi = 0.0, 1.0        # kept() is monotonic in the fraction
        for _ in range(40):
            mid = (lo + hi) / 2
            if kept(mid) > target:
                hi = mid
            else:
                lo = mid
        fraction = lo
    dropped = set()
    for recs in pools.values():
        allow = max(min(len(recs), EMBED_CITY_FLOOR), int(len(recs) * fraction))
        for r in recs[allow:]:
            dropped.add(id(r))
    if not dropped:
        return final
    return [r for r in final if id(r) not in dropped]


def write_data_js(all_inspections, output_path, top_per_city=1000,
                  summaries_budget=8000, merge_from=None):
    """Write a data.js file with window.DATA = [...] for client-side embedding.

    data.js is the map's instant first paint and offline fallback — the
    1,000 most-recently-inspected per city. Completeness comes from the D1
    API (full-city eager loads + viewport queries), not from this file.

    Args:
        merge_from: path to an existing data.js file. Existing records are
                    merged at the restaurant level: a restaurant seen in this
                    run replaces (and extends the score history of) its
                    existing record; restaurants NOT seen in this run are
                    preserved as-is, so weekly refreshes accumulate data
                    instead of wiping it. Records with no inspection in the
                    last STALE_MONTHS months are pruned.
    """
    from collections import defaultdict as _dd

    restaurants = _dd(list)
    for insp in all_inspections:
        rest_id = make_restaurant_id(insp['name'], insp['address'], insp['city'])
        restaurants[rest_id].append(insp)

    records = []
    for rest_id, all_for_rest in restaurants.items():
        inspections = _one_per_day(all_for_rest)
        r = inspections[0]
        lat, lng = _coords_from(r, all_for_rest)
        history = [[i.get('inspection_date', ''), i.get('risk_score', 0)]
                   for i in inspections[:MAX_HISTORY]]
        compact = {
            'n':   r.get('name', ''),
            'a':   r.get('address', ''),
            'c':   r.get('city', ''),
            's':   r.get('state', ''),
            'z':   r.get('zip', ''),
            'lt':  lat,
            'ln':  lng,
            'd':   r.get('inspection_date', ''),
            'os':  r.get('original_score'),
            'rs':  r.get('risk_score', 0),
            'pv':  r.get('priority_violations', 0),
            'pfv': r.get('priority_foundation_violations', 0),
            'cv':  r.get('core_violations', 0),
            'tv':  r.get('total_violations', 0),
            'src': r.get('source', ''),
            'url': r.get('source_url', ''),
            'ic':  len(inspections),
            # Cap violation list size/length to keep the embedded payload small
            'v':   [(list(v)[:2] + [str(list(v)[2])[:250]] if isinstance(v, (list, tuple)) else [v.get('category','unclassified'), v.get('severity','core'), str(v.get('description',''))[:250]]) for v in (r.get('violations') or [])[:12]],
            'i':   rest_id,
            'm':   r.get('metro', ''),
            'h':   history,
        }
        if r.get('_jev'):
            compact['_jev'] = r['_jev']   # the Jev reading of this inspection, stripped before writing
        records.append(compact)

    # Merge with existing data.js if requested (restaurant-level overlay)
    if merge_from:
        existing = _load_existing_data_js(merge_from)
        if existing:
            by_id = {rec['i']: rec for rec in existing if rec.get('i')}
            updated = added = 0
            for new in records:
                old = by_id.get(new['i'])
                if old is None:
                    by_id[new['i']] = new
                    added += 1
                    continue
                # Combine score histories (dedupe by date, newest first)
                hist = {d: s for d, s in (old.get('h') or [[old.get('d', ''), old.get('rs', 0)]]) if d}
                new_dates_added = 0
                for d, s in (new.get('h') or []):
                    if d and d not in hist:
                        new_dates_added += 1
                    if d:
                        hist[d] = s
                combined = sorted(hist.items(), reverse=True)[:MAX_HISTORY]
                # Keep whichever record reflects the most recent inspection
                rec = new if new.get('d', '') >= old.get('d', '') else old
                # Never lose coordinates: a re-fetch during a geocoder outage
                # produces records without lat/lng, but the old record's
                # coordinates are still valid for the same restaurant id.
                for k in ('lt', 'ln'):
                    if rec.get(k) is None and (old if rec is new else new).get(k) is not None:
                        rec[k] = (old if rec is new else new)[k]
                rec['h'] = [[d, s] for d, s in combined]
                rec['ic'] = (old.get('ic') or 1) + new_dates_added
                by_id[new['i']] = rec
                updated += 1
            merged = list(by_id.values())
            cutoff = (datetime.now() - timedelta(days=STALE_MONTHS * 30)).strftime('%Y-%m-%d')
            # Prune stale records and any legacy records with implausible
            # dates (written before the date sanity filter existed)
            records = [rec for rec in merged
                       if rec.get('d', '') >= cutoff and sane_inspection_date(rec.get('d'))]
            log.info(f"Merge: {added} new restaurants, {updated} updated, "
                     f"{len(merged) - len(records)} stale pruned, {len(records)} total")

    # Re-grade every record (fresh AND preserved) so grading-rule changes
    # take effect dataset-wide on the next run rather than only for
    # restaurants that happen to be refetched.
    for rec in records:
        _apply_grades(rec)

    # Cities are (name, state): statewide sources brought a Dallas GA, a
    # Rome GA and 90 other towns whose names already exist under another
    # state, and a shared bucket gave the small one no floor of its own.
    by_city = _dd(list)
    for rec in records:
        by_city[(rec['c'], rec.get('s'))].append(rec)

    final = []
    for city, city_recs in by_city.items():
        # Keep the most recently inspected restaurants per city
        city_recs.sort(key=lambda x: x.get('d', ''), reverse=True)
        final.extend(city_recs[:top_per_city])

    # Global budget on top of the per-city cap: with statewide sources the
    # long tail of small towns dominates, and Cloudflare Pages rejects any
    # asset over 25 MiB. Every city keeps the same fraction of its newest
    # records (`_embed_trim`), never below EMBED_CITY_FLOOR, so every town
    # keeps instant first paint and the metros keep their relative density.
    # Dropping the globally oldest records instead let a few hundred small
    # towns' newest inspections push the metros out one record at a time
    # (measured 2026-11-03: Dallas would have kept ~150 of 1,000). The full
    # roster streams from D1.
    # The floors are on top of the budget (a town's floor never costs a
    # metro its depth); in practice the byte cap below is what binds.
    floors_total = sum(min(len(city_recs), EMBED_CITY_FLOOR) for city_recs in by_city.values())
    if len(final) > EMBED_BUDGET + floors_total:
        before = len(final)
        final = _embed_trim(final, by_city, before - EMBED_BUDGET - floors_total)
        log.info(f"Embed budget: kept {len(final)} records ({before - len(final)} beyond "
                 f"budget live in D1 only)")

    # Global detail budget: embed violation summaries (and the source
    # label/link, which the detail modal can fetch from the API instead)
    # only for the most recently inspected records overall. Everything else
    # keeps map/list essentials; the modal lazy-loads findings for records
    # without 'vs' (same path as lite-loaded areas). A per-city rule can't
    # bound this — with statewide sources most "cities" are small towns.
    final.sort(key=lambda x: x.get('d', ''), reverse=True)
    for rec in final[summaries_budget:]:
        rec.pop('vs', None)
        rec.pop('url', None)
        rec.pop('src', None)

    # Strip the raw violations array before writing: the frontend only reads
    # the vs summaries (which carry capped verbatim text), and 'v' alone is
    # ~40% of the payload. D1 keeps the full violation detail.
    for rec in final:
        rec.pop('v', None)
        rec.pop('_jev', None)
        for s in rec.get('vs') or []:          # records kept from earlier runs too
            if isinstance(s, dict) and s.get('text'):
                s['text'] = display_summary_text(s['text'])

    from datetime import datetime as _dt
    header = f"/* DineScores embedded data — auto-generated {_dt.now().strftime('%Y-%m-%d')} */\n"
    js_content = header + 'window.DATA = ' + json.dumps(final, separators=(',', ':'), default=str) + ';\n'

    if len(js_content) > DATA_JS_MAX_BYTES:
        # Over the byte cap: shrink every city's share (never below the
        # per-city floor) until it fits; D1 still serves them.
        # json.dumps escapes to ASCII, so characters are bytes. The drop
        # count is estimated from the average record and re-measured, a few
        # rounds at most.
        before = len(final)
        for _ in range(8):
            excess = len(js_content) - DATA_JS_MAX_BYTES
            if excess <= 0:
                break
            avg = len(js_content) / max(1, len(final))
            trimmed = _embed_trim(final, by_city, int(excess / avg * 1.05) + 1)
            if len(trimmed) == len(final):
                break   # every city is at its floor
            final = trimmed
            js_content = header + 'window.DATA = ' + json.dumps(final, separators=(',', ':'), default=str) + ';\n'
        log.warning(f"data.js over {DATA_JS_MAX_BYTES:,} bytes: dropped {before - len(final)} records "
                    f"(each city keeps the same share of its newest; they live in D1); now {len(js_content):,} bytes")
        if len(js_content) > DATA_JS_MAX_BYTES:
            log.error(f"data.js is still {len(js_content):,} bytes with only the per-city floors "
                      f"left — lower EMBED_CITY_FLOOR before the next deploy")

    with open(output_path, 'w') as f:
        f.write(js_content)

    log.info(f"Written {len(final)} restaurants to {output_path} ({len(js_content):,} bytes)")



# ─── CLOUDFLARE D1 SQL EXPORT ────────────────────────────────────────────────
# Emits idempotent SQL for Cloudflare D1 (SQLite). The same emitter serves the
# one-time seed load and the weekly incremental refresh: restaurants are
# upserted (newest inspection wins), inspection rows accumulate via
# INSERT OR IGNORE, and inspection_count is recomputed from the inspections
# table so history builds up across runs.

D1_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS restaurants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  address TEXT,
  city TEXT NOT NULL,
  state TEXT,
  zip TEXT,
  lat REAL,
  lng REAL,
  metro TEXT,
  inspection_date TEXT,
  original_score INTEGER,
  risk_score INTEGER,
  weighted_score INTEGER,
  vetted_grade TEXT,
  infractions TEXT,
  summaries TEXT,
  inspection_count INTEGER DEFAULT 1,
  source TEXT,
  source_url TEXT,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_restaurants_city ON restaurants(city);
CREATE INDEX IF NOT EXISTS idx_restaurants_geo ON restaurants(lat, lng);
CREATE INDEX IF NOT EXISTS idx_restaurants_name ON restaurants(name);
CREATE INDEX IF NOT EXISTS idx_restaurants_metro ON restaurants(metro);
CREATE TABLE IF NOT EXISTS inspections (
  id TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL,
  inspection_date TEXT,
  risk_score INTEGER,
  original_score INTEGER,
  inspection_type TEXT,
  results TEXT,
  violations TEXT,
  source_id TEXT,
  source_url TEXT
);
CREATE INDEX IF NOT EXISTS idx_inspections_restaurant
  ON inspections(restaurant_id, inspection_date DESC);
"""

D1_BATCH_ROWS = 40       # max rows per multi-row INSERT
D1_RECOUNT_IDS = 500     # restaurant ids per inspection_count recount statement
                         # (~10KB of literals, far under the 100KB cap)
D1_BATCH_BYTES = 80_000  # max statement size; D1 rejects statements over 100KB
                         # with SQLITE_TOOBIG, and rows with long violation text
                         # can blow past that in far fewer than 40 rows


def _d1_row_batches(rows):
    """Yield row groups capped at D1_BATCH_ROWS rows and D1_BATCH_BYTES bytes."""
    batch, size = [], 0
    for row in rows:
        if batch and (len(batch) >= D1_BATCH_ROWS
                      or size + len(row) > D1_BATCH_BYTES):
            yield batch
            batch, size = [], 0
        batch.append(row)
        size += len(row) + 2
    if batch:
        yield batch


def _sql_quote(value):
    """SQL-literal encoding for D1: NULL, numbers, or single-quoted strings."""
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace('\x00', '').replace("'", "''")
    return f"'{s}'"


def _sql_json(value):
    """Compact ASCII-safe JSON string literal (or NULL)."""
    if not value:
        return 'NULL'
    return _sql_quote(json.dumps(value, separators=(',', ':'), ensure_ascii=True, default=str))


D1_VIOLATION_LINES = 20   # lines stored per inspection (400 characters each)


def _d1_violation_lines(violations):
    """The lines D1 stores for one inspection: the first D1_VIOLATION_LINES
    in form order, except that a longer list is ordered worst tier first
    (stable within a tier) so the cut keeps the priority findings — Lubbock
    checklists run to 70 lines with priority items anywhere on the form."""
    lines = [list(v) for v in (violations or [])]
    if len(lines) > D1_VIOLATION_LINES:
        rank = {'priority': 0, 'priority_foundation': 1, 'core': 2}
        lines.sort(key=lambda v: rank.get(v[1] if len(v) > 1 else '', 3))
    return lines[:D1_VIOLATION_LINES]


def write_d1_sql(all_inspections, output_path, include_schema=True):
    """
    Write idempotent D1 SQL for this run's inspections: inspection rows first
    (upserts), then restaurant upserts where the newest inspection wins, and
    finally inspection_count recomputed from the inspections table for the
    restaurants in this file only.
    """
    restaurants = defaultdict(list)
    for insp in all_inspections:
        rest_id = insp.get('_id') or make_restaurant_id(
            insp['name'], insp['address'], insp['city'])
        restaurants[rest_id].append(insp)

    now_iso = datetime.now(timezone.utc).isoformat()
    insp_rows = []
    rest_rows = []
    rest_ids = []
    seen_insp_ids = set()

    for rest_id, inspections in restaurants.items():
        # One per date, newest first: the worst of that day (_same_day_rank)
        by_date = {}
        for i in sorted(inspections, key=_same_day_rank, reverse=True):
            d = i.get('inspection_date', '')
            if d and (d not in by_date):
                by_date[d] = i
        ordered = [by_date[d] for d in sorted(by_date, reverse=True)]
        if not ordered:
            continue
        latest = ordered[0]

        for i in ordered:
            insp_id = make_inspection_id(rest_id, i['inspection_date'])
            if insp_id in seen_insp_ids:
                continue
            seen_insp_ids.add(insp_id)
            insp_rows.append('(' + ','.join([
                _sql_quote(insp_id),
                _sql_quote(rest_id),
                _sql_quote(i.get('inspection_date')),
                _sql_quote(i.get('risk_score', 0)),
                _sql_quote(i.get('original_score')),
                _sql_quote(i.get('inspection_type', '')),
                _sql_quote(i.get('results', '')),
                _sql_json([list(v)[:2] + [str(list(v)[2])[:400]]
                           for v in _d1_violation_lines(i.get('violations'))]),
                _sql_quote(i.get('source_id', '')),
                _sql_quote(i.get('source_url', '')),
            ]) + ')')

        violation_text = build_violation_text(latest.get('violations', []))
        jev = latest.get('_jev') or {}
        ws = compute_weighted_score(ordered)
        vg = calculate_vetted_grade(ws)
        if calculate_vetted_grade(latest.get('risk_score', 0)) == 'F' or _text_forces_f(violation_text, jev):
            vg = 'F'
        if jev.get('summaries') is not None:
            summaries = [dict(s) for s in jev['summaries']]
        else:
            summaries = summarize_violations(violation_text)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]
        infractions = jev['chips'] if jev.get('chips') is not None else detect_infractions(violation_text)

        lat, lng = _coords_from(latest, inspections)
        rest_ids.append(rest_id)
        rest_rows.append('(' + ','.join([
            _sql_quote(rest_id),
            _sql_quote(latest.get('name', '')),
            _sql_quote(latest.get('address', '')),
            _sql_quote(latest.get('city', '')),
            _sql_quote(latest.get('state', '')),
            _sql_quote(latest.get('zip', '')),
            _sql_quote(float(lat) if lat else None),
            _sql_quote(float(lng) if lng else None),
            _sql_quote(latest.get('metro', '')),
            _sql_quote(latest.get('inspection_date')),
            _sql_quote(latest.get('original_score')),
            _sql_quote(latest.get('risk_score', 0)),
            _sql_quote(ws),
            _sql_quote(vg),
            _sql_json(infractions),
            _sql_json(summaries),
            _sql_quote(len(ordered)),
            _sql_quote(latest.get('source', '')),
            _sql_quote(latest.get('source_url', '')),
            _sql_quote(now_iso),
        ]) + ')')

    rest_upsert_tail = (
        " ON CONFLICT(id) DO UPDATE SET "
        "name=excluded.name, address=excluded.address, city=excluded.city, "
        "state=excluded.state, zip=excluded.zip, "
        "lat=COALESCE(excluded.lat, restaurants.lat), "
        "lng=COALESCE(excluded.lng, restaurants.lng), "
        "metro=excluded.metro, inspection_date=excluded.inspection_date, "
        "original_score=excluded.original_score, risk_score=excluded.risk_score, "
        "weighted_score=excluded.weighted_score, vetted_grade=excluded.vetted_grade, "
        "infractions=excluded.infractions, summaries=excluded.summaries, "
        "source=excluded.source, source_url=excluded.source_url, "
        "updated_at=excluded.updated_at "
        "WHERE excluded.inspection_date >= restaurants.inspection_date;")

    with open(output_path, 'w') as f:
        if include_schema:
            f.write(D1_SCHEMA_SQL)
        for batch in _d1_row_batches(insp_rows):
            # Upsert (not INSERT OR IGNORE) so a re-fetch that recovers
            # violation details heals an inspection row ingested during a
            # source outage. Post-hardening, fetchers never emit an
            # inspection whose details failed to load, so an upsert can
            # only improve a stored row.
            f.write("INSERT INTO inspections "
                    "(id,restaurant_id,inspection_date,risk_score,original_score,"
                    "inspection_type,results,violations,source_id,source_url) VALUES\n"
                    + ',\n'.join(batch) +
                    " ON CONFLICT(id) DO UPDATE SET "
                    "risk_score=excluded.risk_score, "
                    "original_score=excluded.original_score, "
                    "inspection_type=excluded.inspection_type, "
                    "results=excluded.results, "
                    "violations=excluded.violations, "
                    "source_url=excluded.source_url;\n")
        for batch in _d1_row_batches(rest_rows):
            f.write("INSERT INTO restaurants "
                    "(id,name,address,city,state,zip,lat,lng,metro,inspection_date,"
                    "original_score,risk_score,weighted_score,vetted_grade,"
                    "infractions,summaries,inspection_count,source,source_url,"
                    "updated_at) VALUES\n"
                    + ',\n'.join(batch) + rest_upsert_tail + '\n')
        # Recompute inspection_count from the accumulated inspections table so
        # weekly partial runs don't clobber the true history depth. Scoped to
        # this file's restaurants: the old unscoped form rewrote every
        # restaurant in the database (280k+ rows, 2.5M rows read) on each
        # weekly run, which alone exceeded the D1 free plan's daily write cap.
        for start in range(0, len(rest_ids), D1_RECOUNT_IDS):
            ids = ','.join(_sql_quote(r) for r in rest_ids[start:start + D1_RECOUNT_IDS])
            f.write("UPDATE restaurants SET inspection_count = "
                    "(SELECT COUNT(*) FROM inspections "
                    "WHERE inspections.restaurant_id = restaurants.id) "
                    f"WHERE id IN ({ids});\n")

    log.info(f"Written D1 SQL: {len(rest_rows)} restaurants, {len(insp_rows)} "
             f"inspections to {output_path}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

# ─── SOURCE FRESHNESS (self-healing refresh windows) ─────────────────────────
# --freshness-file keeps a small JSON sidecar (data/source_freshness.json in
# CI) recording, per fetch group and per record source, the latest inspection
# date ever shipped plus each group's last-run outcome. Weekly runs look back
# from that recorded date (bounded) instead of a flat 8 days, so a week lost
# to a blocked portal, a timeout, or a source that publishes in batches is
# refetched the next time the source answers instead of becoming a permanent
# gap. The file is also what the CI job summary reads to flag quiet sources.

# The caps are safety bounds, not the normal window. A group's window starts
# the day before its last SHIPPED inspection, so a source that was blocked or
# went quiet for longer than the cap must still be read back to that day or
# the gap becomes permanent: the 45-day API cap silently dropped LA County's
# July and August 2026 when the county's quarterly extract finally appeared,
# and the 21-day portal cap would have abandoned every MHD week older than
# three weeks the moment the portal became reachable again.
FRESHNESS_LOOKBACK_API_DAYS = 400     # open-data APIs/CSVs: cheap to re-read
FRESHNESS_LOOKBACK_PORTAL_DAYS = 90   # scraped portals: each day costs detail pages;
                                      # the fetch deadline bounds a run, dropped
                                      # (undetailed) days are refetched next time, and
                                      # stalest-first ordering drains a backlog evenly
FRESHNESS_OVERLAP_DAYS = 1            # re-read the latest known day (late edits)

_FRESHNESS = None    # {'sources': {...}, 'groups': {...}} once --freshness-file is given
_RUN_GROUPS = {}     # this run's per-group outcomes


def _load_freshness(path):
    global _FRESHNESS
    data = {}
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        log.info(f"Freshness file {path} not found — will be created")
    except Exception as e:
        log.warning(f"Could not read freshness file {path}: {e} — starting fresh")
    _FRESHNESS = {'sources': dict(data.get('sources') or {}),
                  'groups': dict(data.get('groups') or {})}
    log.info(f"Freshness file: {path} ({len(_FRESHNESS['sources'])} sources, "
             f"{len(_FRESHNESS['groups'])} fetch groups)")


def _group_latest(group, source_labels=None):
    """Latest inspection date recorded for a fetch group ('' if unknown).
    With source_labels (the MHD groups), the EARLIEST of those sources'
    latest dates, so a lagging county isn't masked by a current one."""
    if not _FRESHNESS:
        return ''
    if source_labels:
        dates = [(_FRESHNESS['sources'].get(s) or {}).get('latest', '')
                 for s in source_labels]
        return min(dates) if dates and all(dates) else ''
    return (_FRESHNESS['groups'].get(group) or {}).get('latest', '')


def _source_latest(display_name):
    """Latest shipped inspection recorded for an MHD source ('' if unknown)."""
    if not _FRESHNESS:
        return ''
    return (_FRESHNESS['sources'].get(f'{display_name} Health Dept') or {}).get('latest', '')


def _lookback_since(group, default_start, cap_days, source_labels=None):
    """Window start (datetime) for a weekly fetch group: the earlier of the
    mode's default window and the day before the group's last recorded
    inspection, but never more than cap_days back."""
    latest = _group_latest(group, source_labels)
    if not latest:
        return default_start
    try:
        latest_day = datetime.strptime(latest[:10], '%Y-%m-%d')
    except ValueError:
        return default_start
    candidate = latest_day - timedelta(days=FRESHNESS_OVERLAP_DAYS)
    floor = datetime.now() - timedelta(days=cap_days)
    since = max(min(default_start, candidate), floor)
    if since < default_start:
        log.info(f"{group}: last recorded inspection {latest} — looking back to "
                 f"{since:%Y-%m-%d} instead of {default_start:%Y-%m-%d}")
    return since


# One-line explanations fetchers can leave for the freshness report, keyed by
# fetch group and consumed by the caller that records the outcome. A source
# that answers with zero rows is otherwise indistinguishable from one that is
# broken, which cost 77 days of "LA County is stale" before anyone checked
# that the county simply had not published past its fiscal year end.
_GROUP_NOTES = {}

# A source whose own newest row is older than this gets a note explaining the
# lag, so the weekly report's STALE flag reads as "the source has not
# published" rather than "the fetch is broken". Matches the report's own
# staleness threshold in refresh-data.yml.
SOURCE_LAG_NOTE_DAYS = 21


def _note_source_lag(group, source_max):
    """Record that a source's own newest row is behind our staleness window."""
    if not source_max:
        return
    try:
        age = (datetime.now() - datetime.strptime(str(source_max)[:10], '%Y-%m-%d')).days
    except ValueError:
        return
    if age > SOURCE_LAG_NOTE_DAYS:
        _GROUP_NOTES[group] = (f"source's own newest row is {str(source_max)[:10]} "
                               f"({age}d old); nothing newer published")


# A fetcher that DROPS an inspection it could not read for a transient reason
# (a detail-page ban, a timeout, a 5xx, the fetch deadline) calls
# _hold_freshness with that inspection's date. _record_group then keeps the
# source's recorded latest from moving past the drop, so the next run's
# lookback reads that day again. Without it, shipping the later days of the
# same run advanced the latest beyond the drop and lost it for good.
_MHD_DEADLINE_CUT = set()        # MHD display names whose fetch the deadline cut this run
# source label -> {'date|record key': counted}. `counted` is False when the
# drop was not a failure of that record (the deadline, a breaker or a block
# cut the phase short), True when the record was tried and could not be read.
_FRESHNESS_HOLDS = {}
_FRESHNESS_HOLD_LOCK = threading.Lock()
# A record TRIED and failed this many runs in a row is treated as
# permanently unreadable: it is given up (remembered per source in the
# freshness file as 'given_up', so a later re-read that fails again cannot
# restart its count) and no longer holds the window. The count is per record
# ('hold': {'date', 'failures': {'date|key': runs}}): two bad records a day
# apart cannot take turns holding the window open, and one bad record
# cannot drag a newer record's first failure down with it. Drops the
# deadline or a breaker caused hold without counting: a backlog that drains
# over several runs is never given up. No age limit.
FRESHNESS_HOLD_GIVE_UP_RUNS = 3

# A fetcher that shipped only PART of its window (a breaker tripped, the
# deadline cut its detail phase) reports the group's real status here, since
# it ships records and _record_group would otherwise call the run 'ok'.
_GROUP_STATUS = {}               # group -> (status, detail)


def _hold_freshness(source, inspection_date, counted=True, key='', advance=False):
    """Keep `source`'s recorded latest at or before the day before
    `inspection_date` this run (see _FRESHNESS_HOLDS). `key` names the
    dropped record (its inspection or permit id) so its failures are
    counted on their own; counted=False for a drop the deadline, a breaker
    or a block caused: it holds, but never counts toward giving it up.
    advance=True (window-start holds only) also RAISES the latest to the cap
    when this is the earliest hold: for a fetcher that read everything
    before it, so the next window starts there and not a day earlier."""
    d = str(inspection_date or '')[:10]
    if not source or len(d) != 10:
        return
    k = f'{d}|{key or ""}'
    with _FRESHNESS_HOLD_LOCK:
        drops = _FRESHNESS_HOLDS.setdefault(source, {})
        was_counted, was_advance = drops.get(k, (True, False))
        drops[k] = (was_counted and bool(counted), was_advance or bool(advance))


def _hold_window_start(source, start, counted=True, key='', advance=False):
    """Hold `source` so the next run's lookback starts exactly at `start`
    (not two days before it, which is what holding at `start` itself gives:
    the cap is the day before, and the lookback re-reads one more day). For
    fetchers that must re-read a whole window rather than one day; the hold
    date then repeats run after run, so the give-up count works."""
    try:
        day = datetime.strptime(str(start)[:10], '%Y-%m-%d')
    except ValueError:
        return
    _hold_freshness(source, (day + timedelta(days=1 + FRESHNESS_OVERLAP_DAYS)).strftime('%Y-%m-%d'),
                    counted, key, advance)


def _set_group_status(group, status, detail=''):
    """Report a partial run for `group` (consumed by _record_group)."""
    _GROUP_STATUS[group] = (status, detail)


def _record_group(group, records, status=None, detail=''):
    """Remember a fetch group's outcome this run and advance the recorded
    latest dates (per group and per record source). Only records that will
    actually ship count, so a dropped/undetailed batch is refetched later;
    a source with a freshness hold (a transient drop this run) is kept at
    the day before its earliest drop."""
    if not detail:
        detail = _GROUP_NOTES.pop(group, '')
    reported = _GROUP_STATUS.pop(group, None)
    if status is None and reported:
        status = reported[0]
        detail = '; '.join(x for x in (reported[1], detail) if x)
    latest = max((str(r.get('inspection_date') or '')[:10] for r in records), default='')
    if status is None:
        status = 'ok' if records else 'empty'
    outcome = {'status': status, 'records': len(records), 'latest': latest,
               'detail': (detail or '')[:300],
               'at': datetime.now().strftime('%Y-%m-%d')}
    _RUN_GROUPS[group] = outcome
    if _FRESHNESS is None:
        return
    g = _FRESHNESS['groups'].setdefault(group, {})
    g['last_run'] = outcome
    by_source = defaultdict(str)
    for r in records:
        d = str(r.get('inspection_date') or '')[:10]
        s = r.get('source') or ''
        if s and d > by_source[s]:
            by_source[s] = d
    # Freshness holds for this group's sources: the day before the earliest
    # transient drop caps what this run may record (and pulls an already
    # recorded latest back), so the next lookback re-reads the drop. A record
    # tried and failed FRESHNESS_HOLD_GIVE_UP_RUNS runs in a row is given up
    # for good; the next-earliest drop holds instead.
    caps, advance = {}, set()
    with _FRESHNESS_HOLD_LOCK:
        mine = [s for s in _FRESHNESS_HOLDS
                if s in by_source or (_FRESHNESS['sources'].get(s) or {}).get('group') == group]
        dropped = {s: _FRESHNESS_HOLDS.pop(s) for s in mine}
    keep_from = (datetime.now() - timedelta(days=FRESHNESS_LOOKBACK_API_DAYS)).strftime('%Y-%m-%d')
    for s, drops in dropped.items():
        e = _FRESHNESS['sources'].setdefault(s, {'group': group})
        prev_fail = (e.get('hold') or {}).get('failures') or {}
        given_up = set(e.get('given_up') or [])
        failures, live = {}, set()
        for k in sorted(drops):
            if k in given_up:
                continue
            runs = prev_fail.get(k, 0) + (1 if drops[k][0] else 0)
            if runs >= FRESHNESS_HOLD_GIVE_UP_RUNS:
                given_up.add(k)
                log.warning(f"{group}: {s} has failed to read a record from {k[:10]} "
                            f"({k[11:] or 'unnamed'}) {runs} runs in a row — giving up on it")
                continue
            if runs:
                failures[k] = runs
            live.add(k[:10])
        if live:
            first = min(live)
            e['hold'] = {'date': first, 'failures': failures}
            caps[s] = (datetime.strptime(first, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            if any(v[1] for k, v in drops.items() if k[:10] == first and k not in given_up):
                advance.add(s)
        else:
            e.pop('hold', None)
        given_up = sorted(k for k in given_up if k[:10] >= keep_from)
        if given_up:
            e['given_up'] = given_up[-500:]
        else:
            e.pop('given_up', None)
    for s, cap in caps.items():
        if by_source.get(s, '') > cap:
            by_source[s] = cap
        log.info(f"{group}: holding {s} at {cap} (an inspection from the next day was "
                 f"dropped this run and will be re-read)")
    for s, d in by_source.items():
        e = _FRESHNESS['sources'].setdefault(s, {})
        if d > (e.get('latest') or ''):
            e['latest'] = d
        e['group'] = group
        if s not in caps and s not in dropped:
            e.pop('hold', None)          # read cleanly this run
    for s, cap in caps.items():
        e = _FRESHNESS['sources'][s]
        if s in advance or (e.get('latest') or '') > cap:
            e['latest'] = cap
    # The group's latest is the newest of its sources after the caps — one
    # held source must not make a 15-jurisdiction group look stale.
    group_latest = max((e.get('latest') or '' for e in _FRESHNESS['sources'].values()
                        if e.get('group') == group), default='')
    if caps:
        g['latest'] = group_latest or g.get('latest', '')
    elif group_latest > (g.get('latest') or ''):
        g['latest'] = group_latest


def _write_freshness(path):
    if _FRESHNESS is None:
        return
    out = {
        'updated': datetime.now().strftime('%Y-%m-%d'),
        'groups': dict(sorted(_FRESHNESS['groups'].items())),
        'sources': dict(sorted(_FRESHNESS['sources'].items())),
    }
    if _JEV_STATS is not None:
        out['jev'] = {k: v for k, v in _JEV_STATS.items() if k != 'summary_lines'}
    with open(path, 'w') as f:
        json.dump(out, f, indent=1, sort_keys=True)
        f.write('\n')
    log.info(f"Freshness file written: {path}")


_JEV_STATS = None    # this run's Jev pass outcome, written to the freshness file


def _scrub_secret(text):
    key = os.environ.get('TYPESAFE_API_KEY', '').strip()
    text = str(text)
    return text.replace(key, '[REDACTED]') if key and key in text else text


def _run_jev_pass(all_inspections, args):
    """Read every fetched violation line with Jev when JEV_MODE is on. Any
    failure, including one inside the reader module, leaves the records as
    the regex read them: the refresh never fails or ships less because of
    Jev. The outcome goes to the log and the freshness file ('jev')."""
    global _JEV_STATS
    mode = (os.environ.get('JEV_MODE') or 'off').strip().lower()
    if mode in ('', 'off'):
        return
    t0 = time.time()
    deadline = time.time() + args.jev_deadline_minutes * 60 if args.jev_deadline_minutes else None
    try:
        import jev_reader
        reader = jev_reader.JevReader.from_env(deadline=deadline)
        log.info(f"Jev pass: mode={reader.mode} decide={sorted(reader.decide_set) if reader.mode == 'decide' else []} "
                 f"budget={args.jev_deadline_minutes or 'none'} min")
        # absorbed records keep a reading Jev actually made; an all-None _jev
        # (nothing decided, e.g. a budget cut) is read again
        unread = [r for r in all_inspections
                  if not any(v is not None for v in (r.get('_jev') or {}).values())]
        stats = jev_reader.apply_to_inspections(unread, reader, label=args.mode)
        path = reader.flush_cache(args.mode)
        stats['cache_file'] = os.path.basename(path) if path else None
        stats['status'] = 'ok' if reader.active or reader.mode == 'off' else 'inactive'
        stats['elapsed_s'] = round(time.time() - t0, 1)
        for line in stats.get('summary_lines', []):
            log.info(line)
        _JEV_STATS = stats
    except Exception as e:
        log.error(f"Jev pass failed ({type(e).__name__}: {_scrub_secret(e)[:200]}); "
                  f"today's regex answers every line this run")
        _JEV_STATS = {'mode': mode, 'status': 'error',
                      'error': f'{type(e).__name__}: {_scrub_secret(e)[:200]}',
                      'elapsed_s': round(time.time() - t0, 1)}


def _log_run_outcomes():
    if not _RUN_GROUPS:
        return
    log.info("Fetch outcomes this run:")
    for group, o in sorted(_RUN_GROUPS.items()):
        note = f" — {o['detail']}" if o.get('detail') else ''
        log.info(f"  {group:18} {o['status']:8} {o['records']:6} records  "
                 f"latest {o['latest'] or '-'}{note}")


def main():
    parser = argparse.ArgumentParser(description='DineScores Data Pipeline')
    parser.add_argument('--mode', choices=['full', 'weekly', 'test'], default='test',
                        help='Run mode: full (all data), weekly (last 8 days), test (25 records)')
    parser.add_argument('--cities', nargs='+', default=None,
                        help='Cities to fetch (use "dfw" for all DFW metro cities, or individual slugs like "dallas", "fortworth"). '
                             'Default: chicago nyc sf dfw, or nothing when --input-records is given')
    parser.add_argument('--output', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dinescores_data.json'),
                        help='Output JSON file path')
    parser.add_argument('--output-data-js', default=None,
                        help='Also write a data.js file (window.DATA = ...) for client-side embedding')
    parser.add_argument('--merge-existing-data-js', default=None,
                        help='Path to existing data.js to merge with (restaurant-level merge: '
                             'restaurants seen in this run are updated, all others are preserved). '
                             'Required for weekly refreshes so partial pulls do not wipe the dataset.')
    parser.add_argument('--no-dfw-violations', action='store_true',
                        help='Skip scraping DFW inspection detail pages for violation text (faster)')
    parser.add_argument('--since-date', default=None,
                        help='Override start date (YYYY-MM-DD) for ALL sources — useful '
                             'for backfilling a gap after missed refreshes')
    parser.add_argument('--until-date', default=None,
                        help='Override end date (YYYY-MM-DD) for scraped portal sources')
    parser.add_argument('--output-d1-sql', default=None,
                        help='Write idempotent Cloudflare D1 SQL (schema + upserts) for this '
                             'run\'s data. Used by CI to refresh the D1 database weekly.')
    parser.add_argument('--deadline-minutes', type=float, default=None,
                        help='Soft time budget for the fetch phase. Once exceeded, remaining '
                             'fetch groups are skipped (their existing records survive via the '
                             'merge) so outputs are always written. Used by CI to stay inside '
                             'the job timeout.')
    parser.add_argument('--skip-known-inspections', default=None,
                        help='File of TAB-separated "restaurant_id<TAB>YYYY-MM-DD" pairs '
                             'already stored with verified violation details. MHD fetchers '
                             'drop these before the detail phase so a multi-run backfill '
                             'spends the portal\'s per-run request budget only on new '
                             'inspections (the merge preserves the skipped records).')
    parser.add_argument('--freshness-file', default=None,
                        help='JSON sidecar tracking each source\'s latest shipped inspection '
                             'date and last-run outcome. In weekly mode each source looks '
                             'back from its recorded date (bounded) instead of a flat 8 '
                             'days, so a failed or lagging week refetches itself. Updated '
                             'in place at the end of the run.')
    parser.add_argument('--jev-deadline-minutes', type=float, default=None,
                        help='Time budget for the Jev violation-reading pass that runs after '
                             'the fetch phase (JEV_MODE=shadow|decide). Past it, remaining '
                             'lines are read by the regex as today. Unset = no budget.')
    parser.add_argument('--output-records', default=None,
                        help='Also write every inspection record of this run as JSON (.gz '
                             'allowed) so the run can be absorbed later with --input-records: '
                             'a backfill split across machines or runs is put back together '
                             'into one data.js merge without refetching.')
    parser.add_argument('--input-records', nargs='+', default=None,
                        help='Record files written by --output-records to add to this run before '
                             'the output stage (data.js merge, D1 SQL). With no --cities nothing '
                             'is fetched; the freshness file is not updated for absorbed records.')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging for network interception diagnostics')
    args = parser.parse_args()
    if args.cities is None:
        args.cities = [] if args.input_records else ['chicago', 'nyc', 'sf', 'dfw']
    # A bad record path must fail here, not after hours of fetching
    for path in args.input_records or ():
        if not os.path.isfile(path):
            parser.error(f"--input-records: {path} is not a file")
    if args.output_records:
        try:
            with open(args.output_records, 'ab'):
                pass
        except OSError as e:
            parser.error(f"--output-records: cannot write {args.output_records}: {e}")

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.skip_known_inspections:
        with open(args.skip_known_inspections) as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) == 2 and parts[0] and parts[1]:
                    SKIP_KNOWN_INSPECTIONS.add((parts[0], parts[1][:10]))
        log.info(f"Loaded {len(SKIP_KNOWN_INSPECTIONS)} known inspections to skip")

    if args.deadline_minutes is not None:
        global _FETCH_DEADLINE
        _FETCH_DEADLINE = time.time() + args.deadline_minutes * 60
        log.info(f"Fetch deadline: {args.deadline_minutes:g} minutes")

    if args.freshness_file:
        _load_freshness(args.freshness_file)
    if PORTAL_PROXY_URL:
        log.info("PORTAL_PROXY_URL set — MHD and SNHD requests will use the proxy")
    if PORTAL_RELAY_URL:
        log.info("PORTAL_RELAY_URL set — MHD and SNHD requests will go through "
                 f"the relay Worker{'' if PORTAL_RELAY_TOKEN else ' (no token set)'}")
    elif not PORTAL_PROXY_URL and _wants_portal_sources(args.cities):
        log.warning("Neither PORTAL_RELAY_URL nor PORTAL_PROXY_URL is set: the "
                    "MyHealthDepartment portal 403s GitHub-hosted runners, so "
                    "those metros will be skipped if this is a CI run")

    log.info(f"DineScores Pipeline starting | mode={args.mode} cities={args.cities}")

    # Determine date filter
    since_date = None
    record_limit = None
    
    if args.mode == 'weekly':
        since_date = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%dT00:00:00')
        record_limit = 50000
    elif args.mode == 'test':
        record_limit = 25
    elif args.mode == 'full':
        since_date = '2024-01-01T00:00:00'  # 2024+ for API cities; Dallas handles own range
        record_limit = None

    if args.since_date:
        # Global override: a backfill after missed refreshes needs EVERY
        # source to cover the gap, not just the mode's default window.
        since_date = f"{str(args.since_date)[:10]}T00:00:00"

    def weekly_since(group, cap_days, fmt='%Y-%m-%d', source_labels=None, days=8):
        """Window start for a weekly fetch group, formatted with fmt: the
        mode's default (days back), widened by the freshness lookback when a
        --freshness-file is loaded; --since-date overrides both."""
        if args.since_date:
            return datetime.strptime(str(args.since_date)[:10], '%Y-%m-%d').strftime(fmt)
        default_start = datetime.now() - timedelta(days=days)
        return _lookback_since(group, default_start, cap_days, source_labels).strftime(fmt)

    def api_since(group):
        # Open-data sources: the mode default (None in test mode), widened
        # per group in weekly mode.
        if args.mode == 'weekly':
            return weekly_since(group, FRESHNESS_LOOKBACK_API_DAYS, '%Y-%m-%dT00:00:00')
        return since_date

    all_inspections = []

    # Fetch the Socrata-backed cities concurrently (each is a distinct API host)
    socrata_jobs = {}
    if 'chicago' in args.cities:
        socrata_jobs['Chicago'] = lambda: fetch_chicago(
            since_date=api_since('Chicago'), limit=record_limit or 100000)
    if 'nyc' in args.cities:
        socrata_jobs['NYC'] = lambda: fetch_nyc(
            since_date=api_since('NYC'), limit=(record_limit or 1) * 20 if record_limit else None)
    if 'sf' in args.cities:
        socrata_jobs['SF'] = lambda: fetch_sf(
            since_date=api_since('SF'), limit=record_limit or 30000)
    if 'austin' in args.cities:
        def _austin():
            data = fetch_austin(since_date=api_since('Austin'), limit=record_limit or 100000)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Austin'] = _austin
    if 'boston' in args.cities:
        def _boston():
            data = fetch_boston(since_date=api_since('Boston'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Boston'] = _boston
    if 'seattle' in args.cities:
        def _seattle():
            data = fetch_seattle(since_date=api_since('Seattle'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Seattle'] = _seattle
    if 'florida' in args.cities or 'miami' in args.cities:
        def _florida():
            data = fetch_florida(since_date=(str(api_since('Florida'))[:10] if since_date else None),
                                 limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Florida'] = _florida
    if 'nys' in args.cities or 'newyorkstate' in args.cities:
        def _nys():
            data = fetch_nys(since_date=api_since('NY State'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['NY State'] = _nys
    if 'wake' in args.cities or 'raleigh' in args.cities:
        socrata_jobs['Wake County'] = lambda: fetch_wake(
            since_date=api_since('Wake County'), limit=record_limit)
    if 'la' in args.cities or 'losangeles' in args.cities:
        def _la():
            data = fetch_la(since_date=api_since('LA County'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['LA County'] = _la
    if 'louisville' in args.cities:
        def _louisville():
            data = fetch_louisville(since_date=api_since('Louisville'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Louisville'] = _louisville
    if 'minneapolis' in args.cities:
        socrata_jobs['Minneapolis'] = lambda: fetch_minneapolis(
            since_date=api_since('Minneapolis'), limit=record_limit)
    if 'sandiego' in args.cities:
        socrata_jobs['San Diego County'] = lambda: fetch_san_diego(
            since_date=api_since('San Diego County'), limit=record_limit)
    if 'cincinnati' in args.cities:
        socrata_jobs['Cincinnati'] = lambda: fetch_cincinnati(
            since_date=api_since('Cincinnati'), limit=record_limit)
    if 'delaware' in args.cities:
        socrata_jobs['Delaware'] = lambda: fetch_delaware(
            since_date=api_since('Delaware'), limit=record_limit)
    if 'montgomery' in args.cities or 'moco' in args.cities:
        socrata_jobs['Montgomery County'] = lambda: fetch_montgomery(
            since_date=api_since('Montgomery County'), limit=record_limit)
    if 'sc' in args.cities or 'southcarolina' in args.cities:
        socrata_jobs['South Carolina'] = lambda: fetch_sc(
            since_date=api_since('South Carolina'), limit=record_limit)
    if 'fairfax' in args.cities:
        socrata_jobs['Fairfax County'] = lambda: fetch_fairfax(
            since_date=api_since('Fairfax County'), limit=record_limit)
    if 'detroit' in args.cities:
        socrata_jobs['Detroit'] = lambda: fetch_detroit(
            since_date=api_since('Detroit'), limit=record_limit)
    if 'omaha' in args.cities:
        def _omaha():
            # ~9% of the county's rows carry no geometry (its geocoder
            # missed the address); fill those the usual way.
            data = fetch_omaha(since_date=api_since('Omaha'), limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Omaha'] = _omaha
    if 'sacramento' in args.cities:
        socrata_jobs['Sacramento'] = lambda: fetch_sacramento(
            since_date=api_since('Sacramento'), limit=record_limit)
    if 'santaclara' in args.cities or 'sanjose' in args.cities:
        socrata_jobs['Santa Clara County'] = lambda: fetch_santa_clara(
            since_date=api_since('Santa Clara County'), limit=record_limit)
    if 'vegas' in args.cities or 'lasvegas' in args.cities:
        socrata_jobs['Las Vegas'] = lambda: fetch_vegas(
            since_date=api_since('Las Vegas'), limit=record_limit)

    # 2026-11 expansion: every new place is its own fetch group in the pool
    # (the group name is the report row and the freshness key). Records the
    # source published without coordinates are geocoded the usual way; a
    # source whose rows all carry coordinates has nothing to fill.
    def _expansion_job(group, fetcher):
        def run():
            data = fetcher(since_date=api_since(group), limit=record_limit)
            geocode_missing_coords(data)
            return data
        return run
    for slugs, group, fetcher in (
            (('georgia', 'atlanta'), 'Georgia', fetch_georgia),
            (('louisiana', 'neworleans'), 'Louisiana', fetch_louisiana),
            (('maricopa', 'phoenix'), 'Maricopa County', fetch_maricopa),
            (('harris',), 'Harris County', fetch_harris),
            (('pima', 'tucson'), 'Pima County', fetch_pima),
            (('lubbock',), 'Lubbock', fetch_lubbock),
            (('mesquite', 'dfw'), 'Mesquite', fetch_mesquite),
            (('williamson',), 'Williamson County', fetch_williamson),
            (('denton', 'dfw'), 'Denton', fetch_denton),
            (('mckinney', 'dfw'), 'McKinney', fetch_mckinney),
            (('lewisville', 'dfw'), 'Lewisville', fetch_lewisville)):
        if any(s in args.cities for s in slugs):
            socrata_jobs[group] = _expansion_job(group, fetcher)

    # The API-class groups run in a pool WHILE the scraped portals (Tyler,
    # Richardson, Arlington, Orange County, the MHD groups) run on this
    # thread; the pool's results are collected after the portal phase, so
    # all_inspections and the freshness bookkeeping are only touched from
    # this thread. Waiting for the pool first used to be free (its slowest
    # member took ~13 minutes); since 2026-11 it holds Maricopa (~2.5 h) and
    # Louisiana (~1 h), which would have pushed the MHD groups into the
    # deadline's tail every Sunday.
    pool, pool_futures = None, {}
    if socrata_jobs:
        pool = ThreadPoolExecutor(max_workers=len(socrata_jobs))
        pool_futures = {pool.submit(fn): city for city, fn in socrata_jobs.items()}

    # Houston, San Antonio and DC use the Tyler healthinspections.us portals (scraped)
    for slug, fetcher in (('houston', fetch_houston), ('sanantonio', fetch_san_antonio),
                          ('dc', fetch_dc)):
        if slug in args.cities:
            if _past_deadline():
                log.warning(f"{slug}: skipped — pipeline fetch deadline reached")
                _record_group(slug, [], 'skipped', 'fetch deadline reached')
                continue
            try:
                scrape_since = None
                if args.mode == 'weekly':
                    scrape_since = weekly_since(slug, FRESHNESS_LOOKBACK_PORTAL_DAYS)
                elif args.mode == 'full':
                    scrape_since = '2026-01-01'
                if args.since_date:
                    scrape_since = args.since_date
                data = fetcher(since_date=scrape_since, until_date=args.until_date,
                               limit=record_limit,
                               fetch_violations=not args.no_dfw_violations)
                geocode_missing_coords(data)
                all_inspections.extend(data)
                log.info(f"{slug}: {len(data)} records")
                _record_group(slug, data)
            except Exception as e:
                log.error(f"{slug} fetch failed: {e}")
                _record_group(slug, [], 'error', str(e))

    # Richardson uses its own HealthTrak source (not the MHD portal)
    if ('richardson' in args.cities or 'dfw' in args.cities) and _past_deadline():
        log.warning("Richardson: skipped — pipeline fetch deadline reached")
        _record_group('Richardson', [], 'skipped', 'fetch deadline reached')
    elif 'richardson' in args.cities or 'dfw' in args.cities:
        try:
            rich_since = None
            if args.mode == 'weekly':
                rich_since = weekly_since('Richardson', FRESHNESS_LOOKBACK_PORTAL_DAYS)
            elif args.mode == 'full':
                rich_since = '2026-01-01'
            if args.since_date:
                rich_since = args.since_date
            rich_data = fetch_richardson(since_date=rich_since, until_date=args.until_date,
                                         limit=record_limit,
                                         fetch_violations=not args.no_dfw_violations)
            geocode_missing_coords(rich_data)
            all_inspections.extend(rich_data)
            log.info(f"Richardson: {len(rich_data)} records")
            _record_group('Richardson', rich_data)
        except Exception as e:
            log.error(f"Richardson fetch failed: {e}")
            _record_group('Richardson', [], 'error', str(e))

    # Arlington uses its own ArcGIS source (not the MHD portal)
    if ('arlington' in args.cities or 'dfw' in args.cities) and _past_deadline():
        log.warning("Arlington: skipped — pipeline fetch deadline reached")
        _record_group('Arlington', [], 'skipped', 'fetch deadline reached')
    elif 'arlington' in args.cities or 'dfw' in args.cities:
        try:
            arl_since = None
            if args.mode == 'weekly':
                arl_since = weekly_since('Arlington', FRESHNESS_LOOKBACK_API_DAYS)
            if args.since_date:
                arl_since = args.since_date
            arl_data = fetch_arlington(since_date=arl_since, limit=record_limit)
            all_inspections.extend(arl_data)
            log.info(f"Arlington: {len(arl_data)} records")
            _record_group('Arlington', arl_data)
        except Exception as e:
            log.error(f"Arlington fetch failed: {e}")
            _record_group('Arlington', [], 'error', str(e))

    # Orange County, CA: the newest-225 poll + sidecar (see fetch_orange_county).
    # The daily poll-orange-county.yml runs this group alone; the Sunday
    # weekly run reads the same sidecar so data.js and D1 see the whole week.
    if ('orangecounty' in args.cities or 'anaheim' in args.cities) and _past_deadline():
        log.warning("Orange County: skipped — pipeline fetch deadline reached")
        _record_group('Orange County', [], 'skipped', 'fetch deadline reached')
    elif 'orangecounty' in args.cities or 'anaheim' in args.cities:
        try:
            oc_since = None
            if args.mode == 'weekly':
                # The whole sidecar (its last OC_SIDECAR_KEEP_DAYS days), not
                # a lookback window: inspections wait there until their
                # printable is read, so a later poll can make one shippable
                # days after it was first seen; re-upserting is idempotent.
                oc_since = None
            elif args.mode == 'full':
                oc_since = '2026-01-01'
            if args.since_date:
                oc_since = args.since_date
            # The portal has no coordinates. The fetcher geocodes only the
            # sidecar's inspections that still lack them and stores the
            # result there, so each restaurant is geocoded once, not daily.
            oc_data = fetch_orange_county(since_date=oc_since, limit=record_limit,
                                          fetch_violations=not args.no_dfw_violations,
                                          geocode=geocode_missing_coords)
            all_inspections.extend(oc_data)
            log.info(f"Orange County: {len(oc_data)} records")
            # A failed/blocked/skipped poll still ships the sidecar's records,
            # but is reported as such so the daily job fails loudly.
            oc_status, oc_detail = _oc_group_outcome()
            _record_group('Orange County', oc_data, oc_status, oc_detail)
        except Exception as e:
            log.error(f"Orange County fetch failed: {e}")
            _record_group('Orange County', [], 'error', str(e))

    # The MyHealthDepartment groups — the non-DFW metros (Portland, CO Front
    # Range, Utah County, Yolo, ...; one group per distinct config 'group',
    # so the heavy 'MHD metros (midweek)' jurisdictions can run on their own
    # cron) and DFW — share one portal, one ban budget and one fetch
    # deadline, so whichever group runs later is the one a tight run
    # shortchanges. Build them all, then run the group whose recorded data
    # is oldest FIRST (inside a group, fetch_dfw orders its jurisdictions
    # the same way).
    mhd_metro_slugs = _mhd_metro_keys(args.cities)
    dfw_slugs_requested = [c for c in args.cities if c in DFW_JURISDICTIONS]

    mhd_groups = []   # (group name, jurisdictions)
    for group, juris in _mhd_metro_groups(mhd_metro_slugs).items():
        mhd_groups.append((group, juris))
    if 'dfw' in args.cities or dfw_slugs_requested:
        juris = DFW_JURISDICTIONS if 'dfw' in args.cities else {
            s: DFW_JURISDICTIONS[s] for s in dfw_slugs_requested}
        mhd_groups.append(('DFW', juris))

    def _group_key(entry):
        group, juris = entry
        return _group_latest(group, [f"{c['display_name']} Health Dept" for c in juris.values()])

    for group, jurisdictions in sorted(mhd_groups, key=_group_key):
        if _past_deadline():
            log.warning(f"{group}: skipped — pipeline fetch deadline reached")
            _record_group(group, [], 'skipped', 'fetch deadline reached')
            continue
        try:
            mhd_since = None
            if args.mode == 'full':
                mhd_since = '2026-01-01'
            elif args.mode == 'weekly':
                # One window per jurisdiction: its own recorded latest, and
                # its own re-read lag (config 'lag_days'; the Oregon counties
                # publish up to two weeks late and carry 21, everyone else
                # re-reads 8 days). A single group-wide window made every
                # jurisdiction redo the laggiest one's three weeks of detail
                # pages each Sunday.
                mhd_since = {
                    slug: weekly_since(
                        group, FRESHNESS_LOOKBACK_PORTAL_DAYS,
                        days=config.get('lag_days', 8),
                        source_labels=[f"{config['display_name']} Health Dept"])
                    for slug, config in jurisdictions.items()}
            if args.since_date:
                mhd_since = args.since_date
            not_fetched = []
            mhd_data = fetch_dfw(
                jurisdictions=jurisdictions,
                since_date=mhd_since,
                until_date=args.until_date,
                limit_per_jurisdiction=record_limit,
                fetch_violations=not args.no_dfw_violations,
                skipped=not_fetched)
            geocode_missing_coords(mhd_data)
            all_inspections.extend(mhd_data)
            log.info(f"{group}: {len(mhd_data)} records")
            # A group is only 'ok' when every jurisdiction in it was fetched;
            # one starved by the deadline or the breaker makes the run red.
            if _MHD_PORTAL_BLOCKED:
                status, detail = 'blocked', 'MHD portal blocked this run'
            elif any('(deadline)' in x for x in not_fetched):
                status, detail = 'skipped', 'fetch deadline reached'
            elif not_fetched:
                status, detail = 'error', 'fetch failed'
            else:
                status, detail = None, ''
            if not_fetched:
                detail = f"{detail}: not fetched — {', '.join(not_fetched)}"
            _mhd_group_lag_note(group, jurisdictions, not_fetched)
            _record_group(group, mhd_data, status, detail)
        except Exception as e:
            log.error(f"{group} fetch failed: {e}")
            _record_group(group, [], 'error', str(e))

    for future in as_completed(pool_futures):
        city = pool_futures[future]
        try:
            data = future.result()
            all_inspections.extend(data)
            log.info(f"{city}: {len(data)} records")
            _record_group(city, data)
        except Exception as e:
            log.error(f"{city} fetch failed: {e}")
            _record_group(city, [], 'error', str(e))
    if pool is not None:
        pool.shutdown(wait=True)

    if args.input_records:
        all_inspections.extend(read_records_json(args.input_records))

    # Drop coordinates that are obviously wrong before any output is written
    drop_implausible_coords(all_inspections)

    # Jev violation reading (JEV_MODE=shadow|decide; off by default). Runs
    # after the fetch phase on its own budget and can only ever leave a line
    # read by today's regex; it never fails the run or drops a record.
    _run_jev_pass(all_inspections, args)

    if args.output_records:
        write_records_json(all_inspections, args.output_records)

    log.info(f"\nTotal inspections collected: {len(all_inspections)}")

    # Show city breakdown
    city_counts = defaultdict(int)
    for r in all_inspections:
        city_counts[r['city']] += 1
    for city, count in sorted(city_counts.items()):
        log.info(f"  {city}: {count}")

    # Filter out invalid coordinates
    valid = [r for r in all_inspections if r.get('latitude') and r.get('longitude')]
    no_coords = [r for r in all_inspections if not r.get('latitude') or not r.get('longitude')]

    log.info(f"Records with valid coordinates: {len(valid)}")
    if no_coords:
        log.info(f"Records without coordinates: {len(no_coords)}")
    
    # Save to JSON for review
    restaurants = save_to_json(all_inspections, args.output)
    
    # Optionally write data.js (client-side embedded fallback)
    if args.output_data_js:
        write_data_js(all_inspections, args.output_data_js,
                      merge_from=args.merge_existing_data_js)

    # Optionally write D1 SQL (schema + upserts) for the Cloudflare database
    if args.output_d1_sql:
        write_d1_sql(all_inspections, args.output_d1_sql)

    _log_run_outcomes()
    if args.freshness_file:
        _write_freshness(args.freshness_file)

    log.info("Pipeline complete!")


if __name__ == '__main__':
    main()
