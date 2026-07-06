#!/usr/bin/env python3
"""
DineScores Data Pipeline
========================
Collects health inspection data from Chicago, NYC, SF, and DFW metroplex cities.
DFW cities use the MyHealthDepartment portal (inspections.myhealthdepartment.com).
Stores in Firestore with a multi-inspection history model:
  - /restaurants/{restaurant_id}      → most recent inspection + metadata
  - /restaurants/{restaurant_id}/inspections/{inspection_id} → all inspections (history)

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
        r'personal hygiene', r'hygiene',
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
    # Confirmed NOT on myhealthdepartment.com (checked 2026-07-05 — API returns
    # an error dict instead of records): irving, mckinney, denton, garland,
    # grandprairie, mesquite, carrollton, richardson, allen, lewisville,
    # flowermound, fortworth, arlington.
    #   Fort Worth / Arlington — use https://publichealth.tarrantcounty.com/
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
    r'droppings?|excreta|feces)'
    r'[^.]{0,60}?\b(?:rats?|mice|mouse|roach(?:es)?|cockroach(?:es)?|rodents?|vermin)\b',
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

    if re.search(r'\b(vermin|roach|rodent|rats?|mice|insect|fly|flies|gnat|pest)\b', lower):
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
            return []
        if r.status_code != 200:
            return []
        r.encoding = 'utf-8'  # pages omit charset; default latin-1 mangles § etc.
        return _mhd_parse_detail_html(r.text)
    except Exception as e:
        log.debug(f"  Detail fetch failed for {inspection_id}: {e}")
        return []


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

    fetched = 0
    done = 0
    with ThreadPoolExecutor(max_workers=MHD_DETAIL_WORKERS) as pool:
        futures = [pool.submit(fetch_one, rec) for rec in records]
        for future in as_completed(futures):
            rec, texts = future.result()
            done += 1
            if texts:
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
                fetched += 1
            if done % 100 == 0:
                log.info(f"  Details: {done}/{len(records)} inspections fetched "
                         f"({fetched} with violations)")
    return fetched


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
        fetched_violations = _mhd_fetch_details_bulk(
            slug, inspections_with_id,
            trust_official_score=config.get('score_scale', '100') != 'demerit')
        log.info(f"{display_name}: {fetched_violations}/{len(inspections_with_id)} "
                 f"inspections had violation details")

    if stopped_early:
        log.warning(f"{display_name}: scrape stopped early — run again to get remaining data")

    log.info(f"{display_name}: {len(all_records)} total inspection records")
    return all_records


def _parse_mhd_record(rec, slug, display_name, default_city, default_state,
                      score_scale='100'):
    """Parse a single inspection record from the MyHealthDepartment API JSON response."""
    try:
        name = (rec.get('establishmentName') or '').strip()
        if not name or len(name) < 2:
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


def geocode_census_batch(records):
    """
    Batch-geocode US street addresses with the free Census Bureau geocoder
    (up to 10k addresses per request — thousands of times faster than the
    1 req/sec Nominatim loop). Fills latitude/longitude in place and returns
    the number geocoded.
    """
    import csv, io

    geocoded = 0
    chunk_size = 5000
    for chunk_start in range(0, len(records), chunk_size):
        chunk = records[chunk_start:chunk_start + chunk_size]
        rows = io.StringIO()
        writer = csv.writer(rows)
        for idx, r in enumerate(chunk):
            writer.writerow([idx, r.get('address', ''), r.get('city', ''),
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

# ─── DATA MODEL / FIRESTORE UPLOADER ─────────────────────────────────────────

def upload_to_firestore(all_inspections, firebase_creds_path=None, dry_run=False):
    """
    Upload inspections to Firestore using the multi-inspection history model.
    
    Data model:
      /restaurants/{restaurant_id}
        name, address, city, state, zip, latitude, longitude
        latest_inspection_date, risk_score, original_score
        priority_violations, priority_foundation_violations, core_violations
        total_violations, violations (most recent)
        source, inspection_type, results
        inspection_count (total number of inspections in history)
        updated_at
      
      /restaurants/{restaurant_id}/inspections/{inspection_id}
        All fields from above + inspection-specific data
    """
    if dry_run:
        log.info(f"DRY RUN: Would upload {len(all_inspections)} inspections")
        return
    
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        log.error("firebase-admin not installed")
        return
    
    # Initialize Firebase
    if not firebase_admin._apps:
        if firebase_creds_path and os.path.exists(firebase_creds_path):
            cred = credentials.Certificate(firebase_creds_path)
        else:
            # Try Application Default Credentials
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {'projectId': 'healthinspections'})
    
    db = firestore.client()
    
    # Group inspections by restaurant
    restaurants = defaultdict(list)
    for insp in all_inspections:
        rest_id = make_restaurant_id(insp['name'], insp['address'], insp['city'])
        insp['_restaurant_id'] = rest_id
        restaurants[rest_id].append(insp)
    
    log.info(f"Uploading {len(all_inspections)} inspections across {len(restaurants)} unique restaurants...")
    
    batch_size = 400  # Firestore batch limit is 500
    batch = db.batch()
    batch_count = 0
    total_written = 0
    
    for rest_id, inspections in restaurants.items():
        # Sort by date descending (most recent first)
        inspections.sort(key=lambda x: x.get('inspection_date', ''), reverse=True)
        most_recent = inspections[0]
        
        # Restaurant document (most recent inspection data)
        rest_ref = db.collection('restaurants').document(rest_id)

        # ── Vetted grading: weighted score, grade, infractions, summaries ──
        weighted_score = compute_weighted_score(inspections)
        violation_text = build_violation_text(most_recent.get('violations', []))
        vetted_grade = calculate_vetted_grade(weighted_score)
        # Safety override: if latest inspection is F, venue is F
        latest_grade = calculate_vetted_grade(
            most_recent.get('risk_score', 0), violation_text)
        if latest_grade == 'F':
            vetted_grade = 'F'
        infractions = detect_infractions(violation_text)
        violation_summaries = summarize_violations(violation_text)

        rest_data = {
            'id': rest_id,
            'name': most_recent['name'],
            'address': most_recent['address'],
            'city': most_recent['city'],
            'state': most_recent['state'],
            'zip': most_recent.get('zip', ''),
            'latitude': most_recent.get('latitude'),
            'longitude': most_recent.get('longitude'),
            'inspection_date': most_recent['inspection_date'],
            'original_score': most_recent.get('original_score'),
            'risk_score': most_recent['risk_score'],
            'priority_violations': most_recent['priority_violations'],
            'priority_foundation_violations': most_recent['priority_foundation_violations'],
            'core_violations': most_recent['core_violations'],
            'total_violations': most_recent['total_violations'],
            'violations': most_recent['violations'],
            'source': most_recent['source'],
            'source_url': most_recent.get('source_url', ''),
            'inspection_type': most_recent.get('inspection_type', ''),
            'results': most_recent.get('results', ''),
            'metro': most_recent.get('metro', ''),
            'inspection_count': len(inspections),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            # ── New vetted grading fields ──
            'weighted_score': weighted_score,
            'vetted_grade': vetted_grade,
            'infractions': infractions,
            'violation_summaries': violation_summaries,
        }

        # Optional city-specific fields
        if most_recent.get('nyc_grade'):
            rest_data['nyc_grade'] = most_recent['nyc_grade']
        if most_recent.get('cuisine'):
            rest_data['cuisine'] = most_recent['cuisine']
        if most_recent.get('neighborhood'):
            rest_data['neighborhood'] = most_recent['neighborhood']
        
        # Filter out None lat/lng
        if rest_data['latitude'] is None or rest_data['longitude'] is None:
            del rest_data['latitude']
            del rest_data['longitude']
        
        batch.set(rest_ref, rest_data, merge=True)
        batch_count += 1
        
        # Write each inspection to the history subcollection
        for insp in inspections:
            insp_id = make_inspection_id(rest_id, insp['inspection_date'])
            insp_ref = rest_ref.collection('inspections').document(insp_id)
            
            insp_data = {
                'inspection_id': insp_id,
                'restaurant_id': rest_id,
                'inspection_date': insp['inspection_date'],
                'risk_score': insp['risk_score'],
                'original_score': insp.get('original_score'),
                'priority_violations': insp['priority_violations'],
                'priority_foundation_violations': insp['priority_foundation_violations'],
                'core_violations': insp['core_violations'],
                'total_violations': insp['total_violations'],
                'violations': insp['violations'],
                'inspection_type': insp.get('inspection_type', ''),
                'results': insp.get('results', ''),
                'source': insp['source'],
                'source_url': insp.get('source_url', ''),
                'source_id': insp.get('source_id', ''),
            }
            
            # Remove None values
            insp_data = {k: v for k, v in insp_data.items() if v is not None}
            
            batch.set(insp_ref, insp_data, merge=True)
            batch_count += 1
        
        # Commit batch if full
        if batch_count >= batch_size:
            batch.commit()
            total_written += batch_count
            log.info(f"  Committed batch: {total_written} writes total")
            batch = db.batch()
            batch_count = 0
            time.sleep(0.2)
    
    # Final batch
    if batch_count > 0:
        batch.commit()
        total_written += batch_count
    
    log.info(f"Upload complete: {total_written} total Firestore writes")


def delete_test_restaurant(firebase_creds_path=None):
    """Delete the test restaurant located off the coast of Africa (near 0,0 coordinates)."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        log.error("firebase-admin not installed")
        return
    
    if not firebase_admin._apps:
        if firebase_creds_path and os.path.exists(firebase_creds_path):
            cred = credentials.Certificate(firebase_creds_path)
        else:
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {'projectId': 'healthinspections'})
    
    db = firestore.client()
    
    # Find restaurants with coordinates near (0, 0) — off coast of Africa
    # Anything with lat between -10 and 10 AND lon between -10 and 10 is suspect
    all_docs = db.collection('restaurants').get()
    deleted = 0
    
    for doc in all_docs:
        data = doc.to_dict()
        lat = data.get('latitude', 999)
        lon = data.get('longitude', 999)
        
        if lat is not None and lon is not None:
            if -15 <= lat <= 15 and -15 <= lon <= 15:
                log.info(f"Deleting test restaurant: {data.get('name')} at ({lat}, {lon})")
                doc.reference.delete()
                deleted += 1
    
    log.info(f"Deleted {deleted} test restaurant(s)")
    return deleted


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
    rec['vg'] = vg
    if has_violations:
        rec['inf'] = detect_infractions(violation_text)
        summaries = summarize_violations(violation_text)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]
        rec['vs'] = summaries


def write_data_js(all_inspections, output_path, top_per_city=1500, merge_from=None):
    """Write a data.js file with window.DATA = [...] for client-side embedding.

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

    # Strip the raw violations array before writing: the frontend only reads
    # the vs summaries (which carry capped verbatim text), and 'v' alone is
    # ~40% of the payload. Firestore keeps the full violation detail.
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
  source_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_inspections_restaurant
  ON inspections(restaurant_id, inspection_date DESC);
"""

D1_BATCH_ROWS = 40  # rows per multi-row INSERT (keeps statements well under D1 limits)


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
        for start in range(0, len(insp_rows), D1_BATCH_ROWS):
            batch = insp_rows[start:start + D1_BATCH_ROWS]
            f.write("INSERT OR IGNORE INTO inspections "
                    "(id,restaurant_id,inspection_date,risk_score,original_score,"
                    "inspection_type,results,violations,source_id) VALUES\n"
                    + ',\n'.join(batch) + ';\n')
        for start in range(0, len(rest_rows), D1_BATCH_ROWS):
            batch = rest_rows[start:start + D1_BATCH_ROWS]
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
    parser.add_argument('--mode', choices=['full', 'weekly', 'test', 'delete_test'], default='test',
                        help='Run mode: full (all data), weekly (last 8 days), test (25 records), delete_test (remove bogus records)')
    parser.add_argument('--cities', nargs='+', default=['chicago', 'nyc', 'sf', 'dfw'],
                        help='Cities to fetch (use "dfw" for all DFW metro cities, or individual slugs like "dallas", "fortworth")')
    parser.add_argument('--creds', default=None,
                        help='Path to Firebase service account JSON')
    parser.add_argument('--dry-run', action='store_true',
                        help='Process data but do not write to Firestore')
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
    parser.add_argument('--output-d1-sql', default=None,
                        help='Write idempotent Cloudflare D1 SQL (schema + upserts) for this '
                             'run\'s data. Used by CI to refresh the D1 database weekly.')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging for network interception diagnostics')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    log.info(f"DineScores Pipeline starting | mode={args.mode} cities={args.cities}")
    
    if args.mode == 'delete_test':
        delete_test_restaurant(args.creds)
        return
    
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

    # DFW metroplex — supports 'dfw' (all cities), 'dallas' (single), or any slug
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
    
    # Upload to Firestore
    if not args.dry_run and args.creds:
        upload_to_firestore(all_inspections, args.creds, dry_run=False)
    elif not args.dry_run and not args.creds:
        log.info("No --creds provided — skipping Firestore upload (use --dry-run to suppress this warning)")
    else:
        log.info("Dry run mode — skipping Firestore upload")
    
    log.info("Pipeline complete!")


if __name__ == '__main__':
    main()
