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

import os, sys, json, re, time, math, hashlib, logging, argparse
import html as html_lib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('dinescores')

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
    # Confirmed working on myhealthdepartment.com (re-verified 2026-07-05,
    # including from cloud/CI IPs — the portal no longer blocks them).
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

CHROME_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'


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
    if d.year < 2000 or d > datetime.now() + timedelta(days=2):
        return None
    return s


def _socrata_headers():
    """Optional Socrata app token (higher rate limits). Set SOCRATA_APP_TOKEN env var."""
    token = os.environ.get('SOCRATA_APP_TOKEN', '').strip()
    return {'X-App-Token': token} if token else {}


def _socrata_fetch_pages(base_url, params, limit):
    """Paginate through a Socrata endpoint, returning all rows up to limit."""
    headers = _socrata_headers()
    all_rows = []
    while True:
        r = requests.get(base_url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        batch = r.json()
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


def has_active_pest_or_sewage(text):
    t = text or ''
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
            'text': _paraphrase_violation(clean_raw),
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
        r = requests.get(url, params=params, headers=headers, timeout=5)
        time.sleep(1.1)  # Respect Nominatim rate limit
        if r.status_code == 200:
            results = r.json()
            if results:
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

def fetch_seattle(since_date=None, limit=None):
    """
    Fetch King County (Seattle, Bellevue, Kirkland, ...) food inspections
    (data.kingcounty.gov f29f-zza5). One row per violation — aggregated by
    inspection_serial_num. inspection_score is violation POINTS (0 = clean,
    higher = worse); risk_score = 100 - points. RED violations are food
    safety (priority), BLUE are maintenance (core).
    NOTE: the county's feed last updated 2025-11; the weekly refresh will
    pick new data up automatically if publication resumes.
    """
    log.info(f"Fetching Seattle/King County data (since={since_date}, limit={limit})")
    base_url = 'https://data.kingcounty.gov/resource/f29f-zza5.json'
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

    inspections = {}
    for row in rows:
        serial = row.get('inspection_serial_num') or ''
        insp_date = sane_inspection_date(row.get('inspection_date'))
        if not serial or not insp_date:
            continue
        insp = inspections.setdefault(serial, {
            'name': row.get('name') or row.get('inspection_business_name') or '',
            'address': (row.get('address') or '').strip(),
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
            'source_url': 'https://data.kingcounty.gov/resource/f29f-zza5.json',
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
                wait = 20 * (attempt + 1)
                log.warning(f"  Tyler {r.status_code} on {url[:60]} — retry in {wait}s "
                            f"({attempt + 1}/{TYLER_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
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


def _arcgis_query_all(layer_url, where='1=1', out_fields='*', page_size=2000):
    """Fetch every row of an ArcGIS layer via resultOffset pagination."""
    rows = []
    offset = 0
    while True:
        r = requests.get(layer_url + '/query', params={
            'where': where, 'outFields': out_fields, 'outSR': '4326',
            'orderByFields': 'OBJECTID', 'resultOffset': offset,
            'resultRecordCount': page_size, 'f': 'json',
        }, headers={'User-Agent': CHROME_UA}, timeout=120)
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            raise RuntimeError(f"ArcGIS error: {data['error'].get('message')}")
        feats = data.get('features', [])
        rows.extend(feats)
        if len(feats) < page_size:
            return rows
        offset += page_size
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


# ─── LAS VEGAS (Southern Nevada Health District JSON API) ────────────────────

SNHD_API = ('https://www.southernnevadahealthdistrict.org/wp-json/'
            'snhd-eh-restaurants/v1/restaurants')
SNHD_PAGE = 100          # server caps per_page at 100
SNHD_DETAIL_WORKERS = 4


def _snhd_get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params,
                             headers={'User-Agent': CHROME_UA}, timeout=60)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            if attempt == retries - 1:
                return None
            time.sleep(2 * (attempt + 1))


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

    permits, page = {}, 1
    while True:
        d = _snhd_get(SNHD_API, {'per_page': SNHD_PAGE, 'page': page})
        if d is None:
            raise RuntimeError('SNHD list fetch failed')
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
        time.sleep(0.15)
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
            d = _snhd_get(f'{SNHD_API}/{pn}')
            time.sleep(0.1)
            return pn, d

        done = 0
        with ThreadPoolExecutor(max_workers=SNHD_DETAIL_WORKERS) as pool:
            for future in as_completed([pool.submit(work, pn) for pn in todo]):
                pn, d = future.result()
                done += 1
                if done % 1000 == 0:
                    log.info(f"  SNHD details: {done}/{len(todo)}")
                if d is None:
                    # Unreadable permit: skip rather than record without
                    # violation context (same policy as the Tyler portals).
                    skipped += 1
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
                    if not p_date or (sd and p_date < sd):
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
    if skipped:
        log.warning(f"  SNHD: skipped {skipped} permits with unreadable details")
    log.info(f"Las Vegas (SNHD): {len(results)} inspections")
    return results


# ─── MYHEALTHDEPARTMENT PORTAL SCRAPER (DFW + generic) ─────────────────────
# Uses direct HTTP POST to the portal's JSON API (no browser needed).
# The API is the same one the portal's JavaScript frontend calls.

MHD_BASE_URL = 'https://inspections.myhealthdepartment.com/'
MHD_PAGE_SIZE = 25     # server-side hard cap; larger 'count' values are ignored
MHD_QUERY_CAP = 225    # search API silently truncates a query around 225 records
MHD_DELAY = 0.8        # seconds between search API calls
MHD_RETRY_DELAY = 30   # seconds on 403/429 before retry
MHD_MAX_RETRIES = 5
MHD_WINDOW_DAYS = 7    # initial search window; bisected when the query cap is hit
MHD_DETAIL_WORKERS = 3    # concurrent detail-page fetches
MHD_DETAIL_DELAY = 0.4    # per-worker pause between detail-page fetches


def _mhd_session():
    """Create a requests session with browser-like headers for MHD portal."""
    s = requests.Session()
    s.headers.update({
        'User-Agent': CHROME_UA,
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://inspections.myhealthdepartment.com',
        'Referer': 'https://inspections.myhealthdepartment.com/',
    })
    return s


def _mhd_search_page(session, slug, date_range_str, start_offset, retry_count=0):
    """
    Fetch one page of search results from MHD portal API.

    Args:
        session: requests.Session with browser headers
        slug: portal path (e.g. 'dallas')
        date_range_str: date filter string like '2026-03-01 to 2026-03-01'
        start_offset: pagination offset
        retry_count: current retry attempt

    Returns:
        list of record dicts, or None on permanent failure
    """
    payload = {
        'data': {
            'path': slug,
            'programName': '',
            'filters': {
                'date': date_range_str,
                'purpose': '',
            },
            'start': start_offset,
            'count': MHD_PAGE_SIZE,
            'searchStr': '',
            'lat': 0,
            'lng': 0,
            'sort': {},
        },
        'task': 'searchInspections',
    }
    try:
        r = session.post(MHD_BASE_URL, json=payload, timeout=30)
        if r.status_code in (403, 429):
            if retry_count < MHD_MAX_RETRIES:
                wait = MHD_RETRY_DELAY * (retry_count + 1)
                log.warning(f"  MHD blocked ({r.status_code}). Waiting {wait}s before retry "
                            f"{retry_count + 1}/{MHD_MAX_RETRIES}...")
                time.sleep(wait)
                return _mhd_search_page(session, slug, date_range_str, start_offset, retry_count + 1)
            log.error(f"  MHD still blocked after {MHD_MAX_RETRIES} retries")
            return None
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            log.debug(f"  MHD search returned non-list: {type(data).__name__}")
            return []
        return data
    except requests.RequestException as e:
        if retry_count < MHD_MAX_RETRIES:
            wait = MHD_RETRY_DELAY * (retry_count + 1)
            log.warning(f"  MHD network error: {e}. Waiting {wait}s before retry...")
            time.sleep(wait)
            return _mhd_search_page(session, slug, date_range_str, start_offset, retry_count + 1)
        log.error(f"  MHD failed after {MHD_MAX_RETRIES} retries: {e}")
        return None


def _mhd_fetch_window(session, slug, start_day, end_day):
    """
    Fetch all inspection records for a date window (datetime, datetime),
    paginating through results. The search API silently truncates a query
    around MHD_QUERY_CAP records, so if we collect that many the window is
    bisected into halves and re-fetched recursively.

    Returns a list of raw records, or None on permanent failure.
    """
    date_range = f"{start_day.strftime('%Y-%m-%d')} to {end_day.strftime('%Y-%m-%d')}"
    all_results = []
    start = 0
    while len(all_results) < MHD_QUERY_CAP:
        page = _mhd_search_page(session, slug, date_range, start)
        if page is None:
            return None  # permanent failure
        if not page:
            break
        all_results.extend(page)
        if len(page) < MHD_PAGE_SIZE:
            break
        start += MHD_PAGE_SIZE
        time.sleep(MHD_DELAY)

    if len(all_results) >= MHD_QUERY_CAP and start_day < end_day:
        # Cap hit on a multi-day window: bisect and refetch both halves.
        mid = start_day + (end_day - start_day) / 2
        log.info(f"  {slug} {date_range}: hit query cap, splitting window")
        left = _mhd_fetch_window(session, slug, start_day, mid)
        right = _mhd_fetch_window(session, slug, mid + timedelta(days=1), end_day)
        if left is None or right is None:
            return None
        return left + right

    if len(all_results) >= MHD_QUERY_CAP:
        log.warning(f"  {slug} {date_range} hit the {MHD_QUERY_CAP} record cap on a "
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


def _mhd_fetch_inspection_detail(session, slug, inspection_id, retry_count=0):
    """
    Fetch violation details for a single inspection by scraping its public
    detail page. (The portal has no JSON API for details — the observations
    are rendered server-side into the page HTML, so a plain GET suffices.)

    Returns a list of violation description strings, or empty list on failure.
    """
    url = f'{MHD_BASE_URL}{slug}/inspection/'
    try:
        r = session.get(url, params={'inspectionID': inspection_id}, timeout=30)
        if r.status_code in (403, 429):
            if retry_count < 2:
                time.sleep(MHD_RETRY_DELAY)
                return _mhd_fetch_inspection_detail(session, slug, inspection_id, retry_count + 1)
            return None  # blocked — must not read as "no violations"
        if r.status_code != 200:
            return None
        r.encoding = 'utf-8'  # pages omit charset; default latin-1 mangles § etc.
        return _mhd_parse_detail_html(r.text)
    except Exception as e:
        log.debug(f"  Detail fetch failed for {inspection_id}: {e}")
        return None


def _mhd_fetch_details_bulk(slug, records, trust_official_score=True):
    """
    Fetch violation details for many inspections with a small worker pool.
    Each record with details gets its violations/risk fields updated in place.

    trust_official_score: True for 0-100 jurisdictions whose official score
    already IS a risk score; False for demerit jurisdictions, where the risk
    score is recomputed from the scraped violations instead.
    """
    local = threading.local()

    def get_session():
        if not hasattr(local, 'session'):
            local.session = _mhd_session()
        return local.session

    def fetch_one(rec):
        texts = _mhd_fetch_inspection_detail(get_session(), slug, rec['source_id'])
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

    fetched = 0
    done = 0
    failed = []
    with ThreadPoolExecutor(max_workers=MHD_DETAIL_WORKERS) as pool:
        futures = [pool.submit(fetch_one, rec) for rec in records]
        for future in as_completed(futures):
            rec, texts = future.result()
            done += 1
            if texts is None:
                # Blocked/failed fetch — queue for retry; never record the
                # inspection as violation-free on the strength of a 403.
                failed.append(rec)
            elif texts:
                apply_texts(rec, texts)
                fetched += 1
            if done % 100 == 0:
                log.info(f"  Details: {done}/{len(records)} inspections fetched "
                         f"({fetched} with violations)")

    still_failed = []
    if failed:
        log.warning(f"  {slug}: retrying {len(failed)} failed detail fetches")
        retry_session = _mhd_session()
        for rec in failed:
            texts = _mhd_fetch_inspection_detail(retry_session, slug, rec['source_id'])
            time.sleep(MHD_DETAIL_DELAY)
            if texts is None:
                still_failed.append(rec)
            elif texts:
                apply_texts(rec, texts)
                fetched += 1
    return fetched, still_failed


def fetch_dfw(jurisdictions=None, since_date=None, limit_per_jurisdiction=None,
              fetch_violations=True):
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

    Returns:
        list of inspection record dicts with 'metro': 'DFW'
    """
    if jurisdictions is None:
        jurisdictions = DFW_JURISDICTIONS

    session = _mhd_session()
    all_results = []

    for slug, config in jurisdictions.items():
        display_name = config['display_name']
        log.info(f"--- Fetching {display_name} ({slug}) via API ---")
        try:
            results = _fetch_mhd_jurisdiction_api(
                session, slug, config, since_date, limit_per_jurisdiction,
                fetch_violations)
            all_results.extend(results)
            log.info(f"{display_name}: {len(results)} records")
        except Exception as e:
            log.error(f"{display_name} fetch failed: {e}")
            import traceback
            log.error(traceback.format_exc())
        time.sleep(3)  # polite pause between jurisdictions

    log.info(f"DFW total: {len(all_results)} inspection records across "
             f"{len(jurisdictions)} jurisdictions")
    return all_results


def _fetch_mhd_jurisdiction_api(session, slug, config, since_date=None, limit=None,
                                 fetch_violations=True):
    """
    Fetch all inspections for one MHD jurisdiction using daily date windows
    and the searchInspections API. Optionally fetches violation details.
    """
    display_name = config['display_name']
    default_city = config['default_city']
    default_state = config.get('state', 'TX')

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

    # Build list of multi-day windows (bisected automatically if a window
    # hits the portal's query cap)
    windows = []
    current = range_start
    while current <= range_end:
        window_end = min(current + timedelta(days=MHD_WINDOW_DAYS - 1), range_end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)

    log.info(f"{display_name}: {len(windows)} windows of up to {MHD_WINDOW_DAYS} days from "
             f"{range_start.strftime('%Y-%m-%d')} to {range_end.strftime('%Y-%m-%d')}")

    all_records = []
    seen_ids = set()
    stopped_early = False

    for win_idx, (win_start, win_end) in enumerate(windows):
        if limit and len(all_records) >= limit:
            break

        raw_records = _mhd_fetch_window(session, slug, win_start, win_end)
        if raw_records is None:
            log.error(f"  {display_name}: stopping early due to server errors on "
                      f"{win_start.strftime('%Y-%m-%d')}")
            stopped_early = True
            break

        new_count = 0
        for rec in raw_records:
            parsed = _parse_mhd_record(rec, slug, display_name, default_city,
                                       default_state, config.get('score_scale', '100'))
            if parsed:
                uid = parsed.get('source_id') or f"{parsed['name']}|{parsed['inspection_date']}"
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    all_records.append(parsed)
                    new_count += 1

        log.info(f"  Window {win_idx+1}/{len(windows)} "
                 f"({win_start.strftime('%Y-%m-%d')}..{win_end.strftime('%Y-%m-%d')}): "
                 f"{len(raw_records)} results ({new_count} new). Total: {len(all_records)}")

        time.sleep(MHD_DELAY)

    if limit:
        all_records = all_records[:limit]

    # Scrape violation details from each inspection's public detail page
    if fetch_violations and all_records:
        inspections_with_id = [r for r in all_records if r.get('source_id')]
        log.info(f"{display_name}: fetching violation details for "
                 f"{len(inspections_with_id)} inspections "
                 f"({MHD_DETAIL_WORKERS} workers)...")
        fetched_violations, unreadable = _mhd_fetch_details_bulk(
            slug, inspections_with_id,
            trust_official_score=config.get('score_scale', '100') != 'demerit')
        log.info(f"{display_name}: {fetched_violations}/{len(inspections_with_id)} "
                 f"inspections had violation details")
        if unreadable:
            # An inspection whose observations could not be read must not
            # ship looking violation-free — drop it; a later run (or the
            # next weekly refresh) re-attempts it.
            log.warning(f"{display_name}: dropping {len(unreadable)} inspections "
                        f"with unreadable detail pages")
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


def _parse_mhd_record(rec, slug, display_name, default_city, default_state,
                      score_scale='100'):
    """Parse a single inspection record from the MyHealthDepartment API JSON response."""
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
        zip_code = (rec.get('zip') or '').strip()

        # Inspection date — may be ISO string or timestamp
        raw_date = rec.get('inspectionDate') or rec.get('date') or ''
        date_str = sane_inspection_date(raw_date) or ''
        if not date_str and raw_date:
            try:
                date_str = sane_inspection_date(
                    datetime.fromtimestamp(int(raw_date) / 1000).strftime('%Y-%m-%d')) or ''
            except Exception:
                date_str = ''
        if not date_str:
            return None

        # Score
        score = rec.get('score')
        if score is not None:
            try:
                score = int(float(str(score)))
            except Exception:
                score = None

        if score is None:
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

        # Inspection type / purpose
        insp_type = (rec.get('inspectionType') or '').strip()
        purpose = (rec.get('purpose') or '').strip()

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
            'source': f'{display_name} Health Dept',
            'source_url': f'https://inspections.myhealthdepartment.com/{slug}/inspection/?inspectionID={insp_id}' if insp_id else f'https://inspections.myhealthdepartment.com/{slug}',
            'source_id': insp_id or permit_id,
            'metro': 'DFW',
        }
    except Exception as e:
        log.warning(f"{display_name}: failed to parse record: {e} — {str(rec)[:100]}")
        return None


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


def fetch_richardson(since_date=None, limit=None, fetch_violations=True):
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


def geocode_census_batch(records):
    """
    Batch-geocode US street addresses with the free Census Bureau geocoder
    (up to 10k addresses per request — thousands of times faster than the
    1 req/sec Nominatim loop). Fills latitude/longitude in place and returns
    the number geocoded.
    """
    import csv, io

    # Suite/unit/building tokens confuse the Census matcher — strip them for
    # geocoding only (the record's display address is untouched).
    def match_address(addr):
        cleaned = re.sub(r'\s+(?:ste|suite|unit|bldg|building|fl|floor|rm|#)\.?\s*[\w&-]*\s*$',
                         '', addr or '', flags=re.I)
        return re.sub(r'\s{2,}', ' ', cleaned).strip()

    geocoded = 0
    chunk_size = 5000
    for chunk_start in range(0, len(records), chunk_size):
        chunk = records[chunk_start:chunk_start + chunk_size]
        rows = io.StringIO()
        writer = csv.writer(rows)
        for idx, r in enumerate(chunk):
            writer.writerow([idx, match_address(r.get('address', '')), r.get('city', ''),
                             r.get('state', ''), (r.get('zip', '') or '').split('-')[0]])
        try:
            resp = requests.post(
                'https://geocoding.geo.census.gov/geocoder/locations/addressbatch',
                files={'addressFile': ('addresses.csv', rows.getvalue(), 'text/csv')},
                data={'benchmark': 'Public_AR_Current'},
                timeout=300)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.warning(f"Census batch geocode failed: {e}")
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


def geocode_missing_coords(records, nominatim_fallback_cap=150):
    """
    Geocode records missing latitude/longitude: Census Bureau batch first,
    then a capped Nominatim fallback for the stragglers (1 req/sec).
    """
    missing = [r for r in records if not r.get('latitude') or not r.get('longitude')]
    if not missing:
        log.info("All records have coordinates, no geocoding needed")
        return
    log.info(f"Geocoding {len(missing)} records missing coordinates (Census batch)...")
    geocoded = geocode_census_batch(missing)

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


def _weighted_from_history(history_pairs):
    """Weighted score from [[date, risk_score], ...] sorted newest-first."""
    weights = [0.6, 0.3, 0.1]
    total = 0.0
    total_w = 0.0
    for idx, (_, score) in enumerate(history_pairs[:3]):
        total += (score or 0) * weights[idx]
        total_w += weights[idx]
    return round(total / total_w) if total_w else 0


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
    vg = calculate_vetted_grade(rec['ws'])
    if calculate_vetted_grade(rec.get('rs', 0), violation_text) == 'F':
        vg = 'F'
    if not has_violations and not violation_text and rec.get('vg') == 'F':
        # No violation text to re-check (summaries are only embedded for the
        # most recent slice of each city). A stored F may be evidence-based
        # (pest auto-F) — never upgrade it without new inspection data; a
        # fresh fetch of the restaurant re-grades it from full text.
        vg = 'F'
    rec['vg'] = vg
    if has_violations:
        rec['inf'] = detect_infractions(violation_text)
        summaries = summarize_violations(violation_text)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]
        rec['vs'] = summaries


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
    for rest_id, inspections in restaurants.items():
        inspections.sort(key=lambda x: x.get('inspection_date', ''), reverse=True)
        r = inspections[0]
        history = [[i.get('inspection_date', ''), i.get('risk_score', 0)]
                   for i in inspections[:MAX_HISTORY]]
        compact = {
            'n':   r.get('name', ''),
            'a':   r.get('address', ''),
            'c':   r.get('city', ''),
            's':   r.get('state', ''),
            'z':   r.get('zip', ''),
            'lt':  r.get('latitude'),
            'ln':  r.get('longitude'),
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

    by_city = _dd(list)
    for rec in records:
        by_city[rec['c']].append(rec)

    final = []
    for city, city_recs in by_city.items():
        # Keep the most recently inspected restaurants per city
        city_recs.sort(key=lambda x: x.get('d', ''), reverse=True)
        final.extend(city_recs[:top_per_city])

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

    from datetime import datetime as _dt
    header = f"/* DineScores embedded data — auto-generated {_dt.now().strftime('%Y-%m-%d')} */\n"
    js_content = header + 'window.DATA = ' + json.dumps(final, separators=(',', ':'), default=str) + ';\n'

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


def write_d1_sql(all_inspections, output_path, include_schema=True):
    """
    Write idempotent D1 SQL for this run's inspections: inspection rows first
    (INSERT OR IGNORE), then restaurant upserts where the newest inspection
    wins and inspection_count is recomputed from the inspections table.
    """
    restaurants = defaultdict(list)
    for insp in all_inspections:
        rest_id = insp.get('_id') or make_restaurant_id(
            insp['name'], insp['address'], insp['city'])
        restaurants[rest_id].append(insp)

    now_iso = datetime.now(timezone.utc).isoformat()
    insp_rows = []
    rest_rows = []
    seen_insp_ids = set()

    for rest_id, inspections in restaurants.items():
        # Dedupe by date, newest first
        by_date = {}
        for i in inspections:
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
                           for v in (i.get('violations') or [])[:20]]),
                _sql_quote(i.get('source_id', '')),
                _sql_quote(i.get('source_url', '')),
            ]) + ')')

        violation_text = build_violation_text(latest.get('violations', []))
        ws = compute_weighted_score(ordered)
        vg = calculate_vetted_grade(ws)
        if calculate_vetted_grade(latest.get('risk_score', 0), violation_text) == 'F':
            vg = 'F'
        summaries = summarize_violations(violation_text)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]

        lat = latest.get('latitude')
        lng = latest.get('longitude')
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
            _sql_json(detect_infractions(violation_text)),
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
        # weekly partial runs don't clobber the true history depth.
        f.write("UPDATE restaurants SET inspection_count = "
                "(SELECT COUNT(*) FROM inspections "
                "WHERE inspections.restaurant_id = restaurants.id) "
                "WHERE id IN (SELECT DISTINCT restaurant_id FROM inspections);\n")

    log.info(f"Written D1 SQL: {len(rest_rows)} restaurants, {len(insp_rows)} "
             f"inspections to {output_path}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='DineScores Data Pipeline')
    parser.add_argument('--mode', choices=['full', 'weekly', 'test'], default='test',
                        help='Run mode: full (all data), weekly (last 8 days), test (25 records)')
    parser.add_argument('--cities', nargs='+', default=['chicago', 'nyc', 'sf', 'dfw'],
                        help='Cities to fetch (use "dfw" for all DFW metro cities, or individual slugs like "dallas", "fortworth")')
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
                        help='Override start date (YYYY-MM-DD) for scraped portal sources '
                             '(houston/dc) — useful for chunked, restart-safe backfills')
    parser.add_argument('--until-date', default=None,
                        help='Override end date (YYYY-MM-DD) for scraped portal sources')
    parser.add_argument('--output-d1-sql', default=None,
                        help='Write idempotent Cloudflare D1 SQL (schema + upserts) for this '
                             'run\'s data. Used by CI to refresh the D1 database weekly.')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging for network interception diagnostics')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
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
    
    all_inspections = []

    # Fetch the Socrata-backed cities concurrently (each is a distinct API host)
    socrata_jobs = {}
    if 'chicago' in args.cities:
        socrata_jobs['Chicago'] = lambda: fetch_chicago(
            since_date=since_date, limit=record_limit or 100000)
    if 'nyc' in args.cities:
        socrata_jobs['NYC'] = lambda: fetch_nyc(
            since_date=since_date, limit=(record_limit or 1) * 20 if record_limit else None)
    if 'sf' in args.cities:
        socrata_jobs['SF'] = lambda: fetch_sf(
            since_date=since_date, limit=record_limit or 30000)
    if 'austin' in args.cities:
        def _austin():
            data = fetch_austin(since_date=since_date, limit=record_limit or 100000)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Austin'] = _austin
    if 'boston' in args.cities:
        def _boston():
            data = fetch_boston(since_date=since_date, limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Boston'] = _boston
    if 'seattle' in args.cities:
        socrata_jobs['Seattle'] = lambda: fetch_seattle(
            since_date=since_date, limit=record_limit)
    if 'florida' in args.cities or 'miami' in args.cities:
        def _florida():
            data = fetch_florida(since_date=(str(since_date)[:10] if since_date else None),
                                 limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['Florida'] = _florida
    if 'nys' in args.cities or 'newyorkstate' in args.cities:
        def _nys():
            data = fetch_nys(since_date=since_date, limit=record_limit)
            geocode_missing_coords(data)
            return data
        socrata_jobs['NY State'] = _nys
    if 'wake' in args.cities or 'raleigh' in args.cities:
        socrata_jobs['Wake County'] = lambda: fetch_wake(
            since_date=since_date, limit=record_limit)
    if 'vegas' in args.cities or 'lasvegas' in args.cities:
        socrata_jobs['Las Vegas'] = lambda: fetch_vegas(
            since_date=since_date, limit=record_limit)

    if socrata_jobs:
        with ThreadPoolExecutor(max_workers=len(socrata_jobs)) as pool:
            futures = {pool.submit(fn): city for city, fn in socrata_jobs.items()}
            for future in as_completed(futures):
                city = futures[future]
                try:
                    data = future.result()
                    all_inspections.extend(data)
                    log.info(f"{city}: {len(data)} records")
                except Exception as e:
                    log.error(f"{city} fetch failed: {e}")

    # Houston and DC use the Tyler healthinspections.us portals (scraped)
    for slug, fetcher in (('houston', fetch_houston), ('dc', fetch_dc)):
        if slug in args.cities:
            try:
                scrape_since = None
                if args.mode == 'weekly':
                    scrape_since = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
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
            except Exception as e:
                log.error(f"{slug} fetch failed: {e}")

    # Richardson uses its own HealthTrak source (not the MHD portal)
    if 'richardson' in args.cities or 'dfw' in args.cities:
        try:
            rich_since = None
            if args.mode == 'weekly':
                rich_since = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
            elif args.mode == 'full':
                rich_since = '2026-01-01'
            rich_data = fetch_richardson(since_date=rich_since, limit=record_limit,
                                         fetch_violations=not args.no_dfw_violations)
            geocode_missing_coords(rich_data)
            all_inspections.extend(rich_data)
            log.info(f"Richardson: {len(rich_data)} records")
        except Exception as e:
            log.error(f"Richardson fetch failed: {e}")

    # Arlington uses its own ArcGIS source (not the MHD portal)
    if 'arlington' in args.cities or 'dfw' in args.cities:
        try:
            arl_since = None
            if args.mode == 'weekly':
                arl_since = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
            arl_data = fetch_arlington(since_date=arl_since, limit=record_limit)
            all_inspections.extend(arl_data)
            log.info(f"Arlington: {len(arl_data)} records")
        except Exception as e:
            log.error(f"Arlington fetch failed: {e}")

    # DFW metroplex — supports 'dfw' (all MHD cities), 'dallas' (single), or any slug
    dfw_slugs_requested = [c for c in args.cities if c in DFW_JURISDICTIONS]
    if 'dfw' in args.cities or dfw_slugs_requested:
        try:
            # Use consistent ISO YYYY-MM-DD format for dates
            dfw_since = None
            if args.mode == 'full':
                dfw_since = '2026-01-01'
            elif args.mode == 'weekly':
                dfw_since = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

            if 'dfw' in args.cities:
                jurisdictions = DFW_JURISDICTIONS
            else:
                jurisdictions = {s: DFW_JURISDICTIONS[s] for s in dfw_slugs_requested}

            dfw_data = fetch_dfw(
                jurisdictions=jurisdictions,
                since_date=dfw_since,
                limit_per_jurisdiction=record_limit,
                fetch_violations=not args.no_dfw_violations)

            # Geocode records missing coordinates
            geocode_missing_coords(dfw_data)

            all_inspections.extend(dfw_data)
            log.info(f"DFW: {len(dfw_data)} records")
        except Exception as e:
            log.error(f"DFW fetch failed: {e}")

    # Drop coordinates that are obviously wrong before any output is written
    drop_implausible_coords(all_inspections)

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

    log.info("Pipeline complete!")


if __name__ == '__main__':
    main()
