#!/usr/bin/env python3
"""
DineScores Data Pipeline
========================
Collects health inspection data from Chicago, NYC, SF (new 2024+ dataset), and Dallas.
Stores in Firestore with a multi-inspection history model:
  - /restaurants/{restaurant_id}      → most recent inspection + metadata
  - /restaurants/{restaurant_id}/inspections/{inspection_id} → all inspections (history)

Run modes:
  --full    Pull all available data (initial load)
  --weekly  Pull only data from the last 8 days (weekly refresh)
  --test    Pull 25 records per city to verify pipeline works
"""

import os, sys, json, re, time, math, hashlib, logging, argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('dinescores')

# ─── VIOLATION CLASSIFIER ────────────────────────────────────────────────────

VIOLATION_CATEGORIES = {
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
    'pest_control': [
        r'pest', r'rodent', r'rat', r'mouse', r'mice', r'cockroach',
        r'roach', r'fly', r'flies', r'insect', r'vermin', r'bug',
        r'evidence of', r'gnaw', r'droppings', r'bait station',
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
    
    all_records = []
    while True:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_records.extend(batch)
        log.info(f"  Chicago: fetched {len(all_records)} so far...")
        if len(batch) < params['$limit']:
            break
        params['$offset'] += params['$limit']
        time.sleep(0.5)
        if len(all_records) >= limit:
            break
    
    log.info(f"Chicago: {len(all_records)} total inspection records")
    
    # Parse into standard format
    results = []
    for rec in all_records:
        name = rec.get('dba_name') or rec.get('aka_name') or ''
        if not name:
            continue
        
        # Parse violations
        violations_text = rec.get('violations', '')
        violations = []
        if violations_text:
            for part in violations_text.split('|'):
                part = part.strip()
                if part and 'Comments:' in part:
                    desc = part.split('Comments:')[-1].strip()
                    if desc and len(desc) > 5:
                        violations.append(classify_violation(desc))
        
        risk_score, pv, pfv, cv = calc_risk_score(violations)
        
        lat = rec.get('latitude')
        lon = rec.get('longitude')
        
        insp_date = rec.get('inspection_date', '')[:10]
        
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
        '$limit': 50000,
        '$offset': 0,
    }
    if where_clauses:
        params['$where'] = ' AND '.join(where_clauses)
    
    all_rows = []
    while True:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_rows.extend(batch)
        log.info(f"  NYC: fetched {len(all_rows)} rows so far...")
        if len(batch) < params['$limit']:
            break
        params['$offset'] += params['$limit']
        time.sleep(0.5)
        if len(all_rows) >= limit:
            break
    
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
        insp_date = row.get('inspection_date', '')[:10]
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
    
    all_records = []
    while True:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_records.extend(batch)
        log.info(f"  SF: fetched {len(all_records)} so far...")
        if len(batch) < params['$limit']:
            break
        params['$offset'] += params['$limit']
        time.sleep(0.5)
        if len(all_records) >= limit:
            break
    
    log.info(f"SF: {len(all_records)} total inspection records")
    
    results = []
    for rec in all_records:
        name = rec.get('dba', '').strip()
        if not name:
            continue
        
        # Parse SF violation codes and descriptions
        violations = []
        violation_text = rec.get('violation_codes', '') or ''
        if violation_text:
            # Split on commas that are followed by code numbers (e.g., "..., 114266, ...")
            # SF format: "code1, code2, ... - description. code3, code4 - description."
            # Split on " - " to get description blocks
            parts = violation_text.split(' - ')
            for i in range(1, len(parts)):
                desc = parts[i].strip()
                # Clean up: remove code numbers at start of next segment
                desc = re.sub(r'^[0-9,.()\s]+', '', desc).strip()
                if desc and len(desc) > 10:
                    violations.append(classify_violation(desc))
        
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
        
        insp_date = rec.get('inspection_date', '')[:10]
        
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
        })
    
    return results

# ─── DALLAS DATA COLLECTOR (SCRAPER) ─────────────────────────────────────────

def fetch_dallas(since_date=None, limit=None):
    """
    Scrape Dallas health inspections from the myhealthdepartment.com portal.
    Portal uses Flatpickr date range (#filterdate) and flex-row cards.
    Pagination via 'Load More Results' button.
    """
    log.info(f"Fetching Dallas data via scraper (since={since_date})")
    
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return []
    
    results = []
    
    # Build date range strings in MM/DD/YYYY format
    if since_date:
        # since_date may come in as YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
        try:
            dt = datetime.strptime(since_date[:10], '%Y-%m-%d')
            start_date = dt.strftime('%m/%d/%Y')
        except Exception:
            start_date = '01/01/2026'
    else:
        start_date = '01/01/2026'
    
    end_date = datetime.now().strftime('%m/%d/%Y')
    date_range_str = f"{start_date} to {end_date}"
    
    log.info(f"Dallas scrape range: {start_date} to {end_date}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            page.goto('https://inspections.myhealthdepartment.com/dallas', timeout=30000)
            page.wait_for_load_state('networkidle', timeout=20000)
            log.info("Dallas portal loaded")
            
            # Set date range via JavaScript.
            # The portal uses Flatpickr in range mode. Results auto-load when
            # the picker closes with a valid range (onClose callback fires the AJAX).
            # Strategy: setDate() to set both dates, then call close() to trigger
            # the onClose/onChange handlers that the portal listens to.
            try:
                page.evaluate(f"""
                    (function() {{
                        var input = document.getElementById('filterdate');
                        if (!input) {{ console.error('filterdate not found'); return; }}
                        var fp = input._flatpickr;
                        if (!fp) {{ console.error('_flatpickr not attached'); return; }}
                        // setDate(dates, triggerChange) — true fires onChange
                        fp.setDate(['{start_date}', '{end_date}'], true);
                        // close() fires onClose, which the portal uses to reload results
                        fp.close();
                        console.log('Flatpickr set+closed: ' + input.value);
                    }})();
                """)
                log.info(f"Dallas Flatpickr set+closed: {start_date} to {end_date}")
                # Wait for the AJAX results to load
                time.sleep(2)
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(2)
                
                # Verify the input value shows the full range
                actual_value = page.evaluate("document.getElementById('filterdate').value")
                log.info(f"filterdate value after set: '{actual_value}'")
                
                if ' to ' not in (actual_value or ''):
                    log.warning(f"Flatpickr range incomplete (got '{actual_value}') — retrying with change event")
                    # Fallback: set value string directly and fire change
                    page.evaluate(f"""
                        (function() {{
                            var input = document.getElementById('filterdate');
                            var fp = input._flatpickr;
                            if (fp) {{
                                fp.setDate(['{start_date}', '{end_date}'], true);
                                fp.close();
                            }}
                            var setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                            setter.call(input, '{start_date} to {end_date}');
                            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            console.log('Fallback change fired: ' + input.value);
                        }})();
                    """)
                    time.sleep(2)
                    page.wait_for_load_state('networkidle', timeout=30000)
                    time.sleep(2)
                    actual_value = page.evaluate("document.getElementById('filterdate').value")
                    log.info(f"filterdate value after fallback: '{actual_value}'")
                
                log.info(f"Dallas date filter ready: {actual_value}")
            except Exception as e:
                log.warning(f"Date filter setup failed (will use page defaults): {e}")
            
            # Debug: save screenshot and page source so we can see what the browser sees
            import os as _os
            debug_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'logs')
            _os.makedirs(debug_dir, exist_ok=True)
            screenshot_path = _os.path.join(debug_dir, 'dallas_debug.png')
            source_path = _os.path.join(debug_dir, 'dallas_debug.html')
            page.screenshot(path=screenshot_path, full_page=True)
            with open(source_path, 'w', encoding='utf-8') as _f:
                _f.write(page.content())
            log.info(f"Debug screenshot saved: {screenshot_path}")
            log.info(f"Debug page source saved: {source_path}")
            
            # Check what's actually on the page
            flex_count = page.evaluate("document.querySelectorAll('div.flex-row').length")
            log.info(f"div.flex-row count on page: {flex_count}")
            establish_section = page.evaluate("""
                var el = document.querySelector('.section---establishment-list');
                el ? el.innerHTML.substring(0, 300) : 'NOT FOUND'
            """)
            log.info(f"Establishment list HTML: {establish_section}")
            
            if flex_count == 0:
                log.warning("No results visible yet — waiting additional 15s")
                time.sleep(15)
                flex_count = page.evaluate("document.querySelectorAll('div.flex-row').length")
                log.info(f"div.flex-row count after extra wait: {flex_count}")
            
            if flex_count == 0:
                log.error("Still no results after extended wait. Check logs/dallas_debug.png for what the browser sees.")
                raise Exception("No div.flex-row results found on page")
            
            log.info(f"Results confirmed: {flex_count} rows visible, proceeding to extract")
            
            # Click 'Load More Results' until all results are loaded
            load_more_clicks = 0
            max_clicks = 500 if limit is None else math.ceil((limit or 1000) / 20)
            
            while load_more_clicks < max_clicks:
                load_more_btn = page.query_selector('button.load-more-results-button:visible')
                if not load_more_btn:
                    log.info("Dallas: no more 'Load More' button — all results loaded")
                    break
                try:
                    load_more_btn.scroll_into_view_if_needed()
                    load_more_btn.click()
                    page.wait_for_load_state('networkidle', timeout=10000)
                    time.sleep(0.5)
                    load_more_clicks += 1
                    current_count = len(page.query_selector_all('div.flex-row'))
                    log.info(f"Dallas: loaded more ({load_more_clicks} clicks, ~{current_count} rows visible)")
                    if limit and current_count >= limit:
                        break
                except Exception as e:
                    log.warning(f"Dallas load-more error: {e}")
                    break
            
            # Extract all results now that they're all on the page
            results = _extract_dallas_page(page)
            log.info(f"Dallas: extracted {len(results)} records")
        
        except Exception as e:
            log.error(f"Dallas scraper error: {e}")
            import traceback
            log.error(traceback.format_exc())
        finally:
            browser.close()
    
    log.info(f"Dallas: scraped {len(results)} total inspections")
    return results


def _extract_dallas_page(page):
    """Extract all inspection records from the Dallas portal page using correct selectors.
    Results are div.flex-row cards. All should already be loaded via 'Load More' clicks.
    """
    results = []
    rows = page.query_selector_all('div.flex-row')
    log.info(f"Dallas: found {len(rows)} flex-row cards")
    
    for row in rows:
        try:
            # Name: h4.establishment-list-name > a
            name_el = row.query_selector('h4.establishment-list-name a')
            if not name_el:
                name_el = row.query_selector('h4.establishment-list-name')
            name = name_el.inner_text().strip() if name_el else None
            if not name or len(name) < 2:
                continue
            
            # Permit ID from the name link href: /dallas/permit/?permitID=...
            permit_id = ''
            if name_el:
                href = name_el.get_attribute('href') or ''
                pm = re.search(r'permitID=([^&]+)', href)
                if pm:
                    permit_id = pm.group(1)
            
            # Address: div.establishment-list-address
            addr_els = row.query_selector_all('div.establishment-list-address')
            address = addr_els[0].inner_text().strip() if addr_els else ''
            # Clean up city/state from address (portal includes "Dallas, TX XXXXX")
            address = re.sub(r',?\s*Dallas,?\s*TX\s*\d*', '', address, flags=re.IGNORECASE).strip()
            
            # Right column: div.text-right elements
            right_divs = row.query_selector_all('div.text-right')
            insp_type = ''
            date_str = ''
            score = None
            
            for div in right_divs:
                text = div.inner_text().strip()
                # Date + score line: "February 28, 2026 | 95"
                date_match = re.search(r'([A-Za-z]+ \d{1,2},?\s*\d{4})', text)
                if date_match:
                    try:
                        date_str = datetime.strptime(date_match.group(1).strip(), '%B %d, %Y').strftime('%Y-%m-%d')
                    except Exception:
                        pass
                    # Score is in a <strong> tag
                    strong_el = div.query_selector('strong')
                    if strong_el:
                        score_text = strong_el.inner_text().strip()
                        score_match = re.search(r'\d+', score_text)
                        if score_match:
                            score = int(score_match.group())
                # Inspection type line: "Retail Food 2025 | Routine"
                elif '|' in text and not date_match:
                    insp_type = text
            
            # Inspection ID from View Inspection link
            insp_id = ''
            view_link = row.query_selector('a.button-2')
            if view_link:
                href = view_link.get_attribute('href') or ''
                im = re.search(r'inspectionID=([^&]+)', href)
                if im:
                    insp_id = im.group(1)
            
            # Risk score — Dallas uses 100-point scale directly
            if score is not None:
                risk_score = score
            else:
                risk_score = 70  # default if no score
            
            results.append({
                'name': name.title(),
                'address': address.title(),
                'city': 'Dallas',
                'state': 'TX',
                'zip': '',
                'latitude': None,
                'longitude': None,
                'inspection_date': date_str,
                'original_score': score,
                'risk_score': risk_score,
                'priority_violations': 0,
                'priority_foundation_violations': 0,
                'core_violations': 0,
                'total_violations': 0,
                'violations': [],
                'source': 'Dallas Consumer Health',
                'source_url': f'https://inspections.myhealthdepartment.com/dallas/inspection/?inspectionID={insp_id}' if insp_id else 'https://inspections.myhealthdepartment.com/dallas',
                'source_id': insp_id or permit_id,
            })
        except Exception as e:
            continue
    
    return results


def _extract_field(element, selector):
    """Try to extract text from a CSS selector within an element."""
    try:
        el = element.query_selector(selector)
        return el.inner_text().strip() if el else None
    except:
        return None

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
            'inspection_count': len(inspections),
            'updated_at': datetime.now(timezone.utc).isoformat(),
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


def write_data_js(all_inspections, output_path, top_per_city=1500):
    """Write a data.js file with window.DATA = [...] for client-side embedding."""
    from collections import defaultdict as _dd
    
    restaurants = _dd(list)
    for insp in all_inspections:
        rest_id = make_restaurant_id(insp['name'], insp['address'], insp['city'])
        restaurants[rest_id].append(insp)
    
    records = []
    for rest_id, inspections in restaurants.items():
        inspections.sort(key=lambda x: x.get('inspection_date', ''), reverse=True)
        r = inspections[0]
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
            'v':   [[v.get('category','unclassified'), v.get('severity','core'), v.get('description','')] for v in (r.get('violations') or [])],
            'i':   rest_id
        }
        records.append(compact)
    
    by_city = _dd(list)
    for rec in records:
        by_city[rec['c']].append(rec)
    
    final = []
    for city, city_recs in by_city.items():
        city_recs.sort(key=lambda x: x['rs'], reverse=True)
        if city.lower() == 'dallas':
            final.extend(city_recs)
        else:
            final.extend(city_recs[:top_per_city])
    
    from datetime import datetime as _dt
    header = f"/* DineScores embedded data — auto-generated {_dt.now().strftime('%Y-%m-%d')} */\n"
    js_content = header + 'window.DATA = ' + json.dumps(final, separators=(',', ':'), default=str) + ';\n'
    
    with open(output_path, 'w') as f:
        f.write(js_content)
    
    log.info(f"Written {len(final)} restaurants to {output_path} ({len(js_content):,} bytes)")



# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='DineScores Data Pipeline')
    parser.add_argument('--mode', choices=['full', 'weekly', 'test', 'delete_test'], default='test',
                        help='Run mode: full (all data), weekly (last 8 days), test (25 records), delete_test (remove bogus records)')
    parser.add_argument('--cities', nargs='+', default=['chicago', 'nyc', 'sf', 'dallas'],
                        help='Cities to fetch')
    parser.add_argument('--creds', default=None,
                        help='Path to Firebase service account JSON')
    parser.add_argument('--dry-run', action='store_true',
                        help='Process data but do not write to Firestore')
    parser.add_argument('--output', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dinescores_data.json'),
                        help='Output JSON file path')
    parser.add_argument('--output-data-js', default=None,
                        help='Also write a data.js file (window.DATA = ...) for client-side embedding')
    args = parser.parse_args()
    
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
    
    # Fetch data from each city
    if 'chicago' in args.cities:
        try:
            chi_data = fetch_chicago(since_date=since_date, limit=record_limit or 100000)
            all_inspections.extend(chi_data)
            log.info(f"Chicago: {len(chi_data)} records")
        except Exception as e:
            log.error(f"Chicago fetch failed: {e}")
    
    if 'nyc' in args.cities:
        try:
            nyc_data = fetch_nyc(since_date=since_date, limit=(record_limit or 1) * 20 if record_limit else None)
            all_inspections.extend(nyc_data)
            log.info(f"NYC: {len(nyc_data)} records")
        except Exception as e:
            log.error(f"NYC fetch failed: {e}")
    
    if 'sf' in args.cities:
        try:
            sf_data = fetch_sf(since_date=since_date, limit=record_limit or 30000)
            all_inspections.extend(sf_data)
            log.info(f"SF: {len(sf_data)} records")
        except Exception as e:
            log.error(f"SF fetch failed: {e}")
    
    if 'dallas' in args.cities:
        try:
            # Dallas: For full mode, go from Jan 1 2026 to present
            dallas_since = '01/01/2026' if args.mode == 'full' else None
            if args.mode == 'weekly':
                dallas_since = (datetime.now() - timedelta(days=8)).strftime('%m/%d/%Y')
            dallas_data = fetch_dallas(since_date=dallas_since, limit=record_limit)
            all_inspections.extend(dallas_data)
            log.info(f"Dallas: {len(dallas_data)} records")
        except Exception as e:
            log.error(f"Dallas fetch failed: {e}")
    
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
    log.info(f"Records without coordinates (Dallas, needs geocoding): {len(no_coords)}")
    
    # Save to JSON for review
    restaurants = save_to_json(all_inspections, args.output)
    
    # Optionally write data.js (client-side embedded fallback)
    if args.output_data_js:
        write_data_js(all_inspections, args.output_data_js)
    
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
