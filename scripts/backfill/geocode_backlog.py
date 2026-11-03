"""geocode_backlog.py [--patch] <out.records.json.gz> <nominatim cap> <records.json.gz>...

Backfilled restaurants the Census batch geocoder could not place (and the
weekly's 150-per-run Nominatim fallback has not reached yet) get one more
pass here: the newest inspection record of every restaurant without
coordinates across the given record files goes through
dinescores_pipeline.geocode_missing_coords with a larger Nominatim cap, at
Nominatim's one-request-per-second pace. The records that gained
coordinates are written to <out> for the absorb step, whose data.js merge
and D1 upserts coalesce coordinates onto the existing rows. With --patch the
output is every input record with the found coordinates filled in (what
absorb.sh feeds to the pipeline instead of the records as fetched).
Run from anywhere (the pipeline module is found next to this file or via DINESCORES_REPO), one instance at a time."""
import gzip, json, sys
import os
sys.path.insert(0, os.environ.get('DINESCORES_REPO') or os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
import dinescores_pipeline as dp

args = [a for a in sys.argv[1:] if a != '--patch']
patch = '--patch' in sys.argv[1:]   # write EVERY input record, coordinates filled in, instead of the placed ones only
out, cap, files = args[0], int(args[1]), args[2:]
all_records, newest = [], {}
for path in files:
    for rec in json.load(gzip.open(path, 'rt')):
        all_records.append(rec)
        if rec.get('latitude') and rec.get('longitude'):
            continue
        key = dp.make_restaurant_id(rec['name'], rec['address'], rec['city'])
        if key not in newest or (rec.get('inspection_date') or '') > (newest[key].get('inspection_date') or ''):
            newest[key] = dict(rec)
todo = list(newest.values())
print(f"{len(todo)} restaurants without coordinates across {len(files)} record files")
dp.geocode_missing_coords(todo, nominatim_fallback_cap=cap)
placed = [r for r in todo if r.get('latitude') and r.get('longitude')]
dp.drop_implausible_coords(placed)
placed = [r for r in placed if r.get('latitude') and r.get('longitude')]
print(f"{len(placed)} placed, {len(todo) - len(placed)} still without coordinates")
if patch:
    coords = {dp.make_restaurant_id(r['name'], r['address'], r['city']): (r['latitude'], r['longitude']) for r in placed}
    for rec in all_records:
        if not rec.get('latitude') or not rec.get('longitude'):
            hit = coords.get(dp.make_restaurant_id(rec['name'], rec['address'], rec['city']))
            if hit:
                rec['latitude'], rec['longitude'] = hit
    dp.write_records_json(all_records, out)
else:
    dp.write_records_json(placed, out)
