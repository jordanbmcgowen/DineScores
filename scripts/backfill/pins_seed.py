#!/usr/bin/env python3
"""pins_seed.py <placed.records.json.gz> --reference <records.json.gz>... \\
        [--seed data/d1_seed_<date>_map_pins.sql.gz] [--data-js public/data.js --write-data-js]

Coordinates found after a backfill was absorbed (geocode_backlog.py, or the
OpenStreetMap pass on its own) go back as coordinates only: one guarded
UPDATE per restaurant (`AND lat IS NULL`, so a pin the weekly run has set in
the meantime is never moved and loading the seed twice changes nothing), and
the same fill for embedded data.js records that have none. Nothing else on a
row moves: re-absorbing the placed records instead would rebuild each
restaurant's weighted score from the one inspection in the file.

A found pin is checked against its own town before it ships. The geocoder is
asked for "address, city, state" as free text and can answer with a street of
the same name elsewhere in the state; the pipeline's own check is the state's
bounding box. Here a pin more than --max-km (40) from the median of the
town's already-placed restaurants (the --reference record files, at least
--min-reference (3) of them in that town) is left out. A town with too few
placed restaurants to compare against keeps its pins.
"""
import argparse
import gzip
import json
import math
import os
import statistics
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.environ.get('DINESCORES_REPO') or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..'))
import dinescores_pipeline as dp   # noqa: E402


def _km(a, b):
    """Great-circle distance between two (lat, lng) pairs."""
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def _load(path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def town_centres(reference_files, min_reference=3):
    """{(city, state): (median lat, median lng)} from records that have coordinates."""
    pts = defaultdict(dict)
    for path in reference_files:
        for r in _load(path):
            if r.get('latitude') and r.get('longitude'):
                rid = dp.make_restaurant_id(r['name'], r['address'], r['city'])
                pts[(r.get('city'), dp.state_code(r.get('state') or ''))][rid] = (
                    float(r['latitude']), float(r['longitude']))
    return {town: (statistics.median(p[0] for p in by.values()), statistics.median(p[1] for p in by.values()))
            for town, by in pts.items() if len(by) >= min_reference}


def choose(placed, centres, max_km=40.0):
    """(kept, stats): {restaurant id: (lat, lng)} for the pins that pass."""
    kept, stats, far = {}, Counter(), []
    for r in placed:
        lat, lng = r.get('latitude'), r.get('longitude')
        if not lat or not lng:
            continue
        lat, lng = float(lat), float(lng)
        state = dp.state_code(r.get('state') or '')
        if not dp.coords_plausible(lat, lng, state):
            stats['outside_the_state'] += 1
            continue
        centre = centres.get((r.get('city'), state))
        if centre is None:
            stats['kept_no_town_reference'] += 1
        elif _km((lat, lng), centre) > max_km:
            stats['too_far_from_its_town'] += 1
            far.append((r.get('name'), r.get('address'), r.get('city'), round(_km((lat, lng), centre))))
            continue
        else:
            stats['kept_near_its_town'] += 1
        kept[dp.make_restaurant_id(r['name'], r['address'], r['city'])] = (lat, lng)
    return kept, stats, far


def seed_sql(pins):
    return ''.join(
        f"UPDATE restaurants SET lat={lat:.6f}, lng={lng:.6f} WHERE id={dp._sql_quote(rid)} AND lat IS NULL;\n"
        for rid, (lat, lng) in sorted(pins.items()))


def fill_data_js(content, pins):
    """(new content, records filled): coordinates for embedded records that have none."""
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip(';\n'))
    filled = 0
    for rec in recs:
        hit = pins.get(rec.get('i'))
        if hit and (rec.get('lt') is None or rec.get('ln') is None):
            rec['lt'], rec['ln'] = round(hit[0], 6), round(hit[1], 6)
            filled += 1
    return head + 'window.DATA = ' + json.dumps(recs, separators=(',', ':'), default=str) + ';\n', filled


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('placed')
    ap.add_argument('--reference', nargs='+', required=True)
    ap.add_argument('--max-km', type=float, default=40.0)
    ap.add_argument('--min-reference', type=int, default=3)
    ap.add_argument('--seed', default=None)
    ap.add_argument('--data-js', default=None)
    ap.add_argument('--write-data-js', action='store_true')
    args = ap.parse_args(argv)

    placed = _load(args.placed)
    centres = town_centres(args.reference, args.min_reference)
    pins, stats, far = choose(placed, centres, args.max_km)
    print(f"{len(placed)} placed records, {len(pins)} pins kept: " +
          ', '.join(f'{k} {v}' for k, v in sorted(stats.items())))
    for name, address, city, km in far[:40]:
        print(f"   left out: {name} | {address}, {city}: {km} km from the town's other restaurants")
    if args.seed:
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(f'-- Map pins for {len(pins)} backfilled restaurants (coordinates only; '
                    f'guarded by lat IS NULL).\n')
            f.write(seed_sql(pins))
        print(f'wrote {len(pins)} guarded updates to {args.seed}')
    if args.data_js:
        with open(args.data_js, encoding='utf-8') as f:
            content = f.read()
        new_content, filled = fill_data_js(content, pins)
        print(f'data.js: {filled} embedded records gain coordinates; {len(content):,} -> {len(new_content):,} bytes')
        if len(new_content) > dp.DATA_JS_MAX_BYTES:
            raise SystemExit(f'data.js would be over {dp.DATA_JS_MAX_BYTES:,} bytes')
        if args.write_data_js:
            with open(args.data_js, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print('data.js written')


if __name__ == '__main__':
    sys.exit(main())
